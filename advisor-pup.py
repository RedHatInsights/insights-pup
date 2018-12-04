import logging
import os
import sys
import asyncio
import base64
import requests
import collections
import json

import aiohttp

from tempfile import NamedTemporaryFile
from aiohttp.client_exceptions import ClientConnectionError
from logstash_formatter import LogstashFormatterV1
from concurrent.futures import ThreadPoolExecutor
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.errors import KafkaError

from insights import run, extract
from insights.specs import Specs

# Logging
if any("KUBERNETES" in k for k in os.environ):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(LogstashFormatterV1())
    logging.root.setLevel(os.getenv("LOGLEVEL", "INFO"))
    logging.root.addHandler(handler)
else:
    logging.basicConfig(
        level=os.getenv("LOGLEVEL", "INFO"),
        format="%(threadname)s %(levelname)s %(name)s - %(message)s"
    )

logger = logging.getLogger('advisor-pup')

# Maxium workers for threaded execution
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 50))

CANONICAL_FACTS = {
    'insights-id': Specs.machine_id,
    'fqdn': Specs.hostname
}

INVENTORY_URL = os.getenv('INVENTORY_URL', 'http://inventory:5000/api/hosts')

MQ = os.getenv('KAFKAMQ', 'kafka:29092').split(',')
MQ_GROUP_ID = os.getenv('MQ_GROUP_ID', 'advisor-pup')
PUP_QUEUE = os.getenv('PUP_QUEUE', 'platform.upload.pup')
RETRY_INTERVAL = int(os.getenv('RETRY_INTERVAL', 5))  # seconds

thread_pool_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
loop = asyncio.get_event_loop

session = aiohttp.ClientSession()


class MQClient(object):

    def __init__(self, client, name):
        self.client = client
        self.name = name
        self.connected = False

    def __str__(self):
        return f"MQClient {self.name} {self.client} {'connected' if self.connected else 'disconnected'}"

    async def start(self):
        while not self.connected:
            try:
                logger.info("Attempting to connect %s client.", self.name)
                await self.client.start()
                logger.info("%s client connected successfully.", self.name)
                self.connected = True
            except KafkaError:
                logger.exception("Failed to connect %s client, retrying in %d seconds.", self.name, RETRY_INTERVAL)
                await asyncio.sleep(RETRY_INTERVAL)

    async def work(self, worker):
        try:
            await worker(self.client)
        except KafkaError:
            logger.exception("Encountered exception while working %s client, reconnecting.", self.name)
            self.connected = False

    def run(self, worker):
        async def _f():
            while True:
                await self.start()
                await self.work(worker)
        return _f


mqc = AIOKafkaConsumer(
    PUP_QUEUE, loop=loop, bootstrap_server=MQ,
    group_id=MQ_GROUP_ID
)
mqp = AIOKafkaProducer(
    loop=loop, bootstrap_servers=MQ, request_timeout_ms=10000,
    connections_max_idle_ms=None
)

CONSUMER = MQClient(mqc, "consumer")
PRODUCER = MQClient(mqp, "producer")

# local queue for pushing items into kafka, this queue fills up if kafka goes down
produce_queue = collections.deque([], '999')


async def consume(client):
    data = await client.getmany()
    for tp, msgs in data.items():
        if tp.topic == PUP_QUEUE:
            await handle_file(msgs)
    await asyncio.sleep(0.1)


async def handle_file(msgs):

    for msg in msgs:
        try:
            data = json.loads(msg.value)
        except ValueError:
            logger.error("handle_file(): unable to decode msg as json: {}".format(msg.value))
            continue

        result = await validate(data['url'])

        if result:
            response = post_to_inventory(result, data)

            produce_queue.append(
                {
                    'topic': 'platform.upload.validation',
                    'msg': {'inventory': response,
                            'facts': result,
                            'payload_id': data['payload_id']}
                }
            )
        else:
            produce_queue.append(
                {'topic': 'platform.upload.validation',
                 'msg': {'payload_id': data['payload_id'],
                         'validation': 'failure'}}
            )


async def send_result(client):
    if not produce_queue:
        await asyncio.sleep(0.1)
    else:
        item = produce_queue.popleft()
        topic, msg = item['topic'], item['msg']
        logger.info(
            "Popped item from produce queue (qsize: %d): topic %s: %s",
            len(produce_queue), topic, msg
        )
        try:
            await client.send_and_wait(topic, json.dumps(msg).encode("utf-8"))
        except KafkaError:
            produce_queue.appendleft(item)
            raise


async def validate(self, url):

    temp = NamedTemporaryFile(delete=False).name

    async with session.get(url) as response:
        open(temp, 'wb').write(await response.read())

    try:
        return await extract_facts(temp)
    finally:
        os.remove(temp)


async def extract_facts(archive):
    facts = {}
    with extract(archive) as ex:
        broker = run(root=ex.tmp_dir)
        for k, v in CANONICAL_FACTS.items():
            facts[k] = ('\n'.join(broker[v].content))

    return facts


async def post_to_inventory(facts, msg):

    post = {**facts, **msg}
    post['account'] = post.pop('rh_account')
    post['canonical_facts'] = {}

    identity = base64.b64encode(str({'account': post['account'], 'org_id': msg['principal']}))
    headers = {'x-rh-identity': identity,
               'Content-Type': 'application/json'}

    try:
        async with session.post(INVENTORY_URL, data=json.dumps(post), headers=headers) as response:
            if response.status != 200 and response.status != 201:
                logger.error('Failed to post to inventory: ' + await response.text())
            else:
                logger.info("Payload posted to inventory: %s", msg['payload_id'])
                return await response.json()
    except ClientConnectionError as e:
        logger.error("Unable to contact inventory: %s", e)
        return {"error": "Unable to update inventory. Service unavailable"}

    inv = requests.post(INVENTORY_URL, data=json.dumps(post), headers=headers)
    if inv.status_code != 201:
        logger.error('Failed to post to inventory: ' + inv.text)


def main():
    try:
        loop.set_default_executor(thread_pool_executor)
        loop.create_task(CONSUMER.run(consume))
        loop.create_task(PRODUCER.run(send_result))
        loop.run_forever()
    except KeyboardInterrupt:
        loop.stop()


if __name__ == "__main__":
    main()
