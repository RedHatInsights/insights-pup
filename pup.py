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

from insights import extract
from insights.util.canonical_facts import get_canonical_facts
from insights.specs import Specs
from insights.core.archives import InvalidContentType

# Logging
if any("KUBERNETES" in k for k in os.environ):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(LogstashFormatterV1())
    logging.root.setLevel(os.getenv("LOGLEVEL", "INFO"))
    logging.root.addHandler(handler)
else:
    logging.basicConfig(
        level=os.getenv("LOGLEVEL", "INFO"),
        format="%(threadName)s %(levelname)s %(name)s - %(message)s"
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
loop = asyncio.get_event_loop()


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
    PUP_QUEUE, loop=loop, bootstrap_servers=MQ,
    group_id=MQ_GROUP_ID
)
mqp = AIOKafkaProducer(
    loop=loop, bootstrap_servers=MQ, request_timeout_ms=10000,
    connections_max_idle_ms=None
)

CONSUMER = MQClient(mqc, "consumer")
PRODUCER = MQClient(mqp, "producer")

# local queue for pushing items into kafka, this queue fills up if kafka goes down
produce_queue = collections.deque([], 999)


async def consume(client):
    data = await client.getmany()
    for tp, msgs in data.items():
        if tp.topic == PUP_QUEUE:
            logger.info("received messages: %s", msgs)
            await handle_file(msgs)
    await asyncio.sleep(0.1)


async def handle_file(msgs):

    for msg in msgs:
        try:
            data = json.loads(msg.value)
        except ValueError:
            logger.error("handle_file(): unable to decode msg as json: {}".format(msg.value))
            continue

        logger.info(data)
        machine_id = data['metadata'].get('machine_id') if data.get('metadata') else None
        result = await validate(data['url'])

        if 'error' not in result:
            if result['insights_id'] != machine_id:
                response = await post_to_inventory(result, data)
            else:
                response = None

            produce_queue.append(
                {
                    'topic': 'platform.upload.validation',
                    'msg': {'id': response.get('id') if response else None,
                            'facts': result,
                            'service': data['service'],
                            'payload_id': data['payload_id'],
                            'validation': 'success'}
                }
            )
        else:
            logger.info("Payload [%s] failed to validate with error: %s", data['payload_id'], result['error'])
            produce_queue.append(
                {'topic': 'platform.upload.validation',
                'msg': {'payload_id': data['payload_id'],
                        'validation': 'failure'}}
            )


def make_producer(queue=None):
    queue = produce_queue if queue is None else queue

    async def send_result(client):
        if not queue:
            await asyncio.sleep(0.1)
        else:
            item = queue.popleft()
            topic, msg = item['topic'], item['msg']
            logger.info(
                "Popped item from produce queue (qsize: %d): topic %s: %s",
                len(queue), topic, msg
            )
            try:
                await client.send_and_wait(topic, json.dumps(msg).encode("utf-8"))
            except KafkaError:
                queue.append(item)
                raise
    return send_result


async def validate(url):

    temp = NamedTemporaryFile(delete=False).name

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            open(temp, 'wb').write(await response.read())

    try:
        return await extract_facts(temp)
    finally:
        os.remove(temp)


async def extract_facts(archive):
    logger.info("extracting facts from %s", archive)
    facts = {}
    try:
        with extract(archive) as ex:
            facts = get_canonical_facts(path=ex.tmp_dir)
    except (InvalidContentType, KeyError) as e:
        facts['error'] = e.args[0]

    return facts


async def post_to_inventory(facts, msg):

    post = {**facts, **msg}
    post['account'] = post.pop('rh_account')
    post['facts'] = []
    if post.get('metadata'):
        post['facts'].append({'facts': post.pop('metadata'),
                              'namespace': 'insights-client'})

    headers = {'x-rh-identity': post['b64_identity'],
               'Content-Type': 'application/json'}

    try:
        async with aiohttp.ClientSession() as session:
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
        loop.create_task(CONSUMER.run(consume)())
        loop.create_task(PRODUCER.run(make_producer(produce_queue))())
        loop.run_forever()
    except KeyboardInterrupt:
        loop.stop()


if __name__ == "__main__":
    main()
