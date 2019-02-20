import logging
import os
import sys
import asyncio
import collections
import json
import requests
import aiohttp

from tempfile import NamedTemporaryFile
from aiohttp.client_exceptions import ClientConnectionError, ServerDisconnectedError
from logstash_formatter import LogstashFormatterV1
from concurrent.futures import ThreadPoolExecutor
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.errors import KafkaError
from kafkahelpers import ReconnectingClient
from prometheus_async.aio import time

from utils import mnm
from insights import extract
from insights.util.canonical_facts import get_canonical_facts
from insights.specs import Specs
from insights.core.archives import InvalidArchive

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

BUILD_ID = os.getenv('OPENSHIFT_BUILD_COMMIT', 'somemadeupvalue')

INVENTORY_URL = os.getenv('INVENTORY_URL', 'http://inventory:5000/api/hosts')

MQ = os.getenv('KAFKAMQ', 'kafka:29092').split(',')
MQ_GROUP_ID = os.getenv('MQ_GROUP_ID', 'advisor-pup')
PUP_QUEUE = os.getenv('PUP_QUEUE', 'platform.upload.pup')
RETRY_INTERVAL = int(os.getenv('RETRY_INTERVAL', 5))  # seconds


def get_commit_date(commit_id):
    url = "https://api.github.com/repos/RedHatInsights/insights-pup/git/commits/" + commit_id
    response = requests.get(url).json()
    return response['committer']['date']


thread_pool_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
loop = asyncio.get_event_loop()

kafka_consumer = AIOKafkaConsumer(
    PUP_QUEUE, loop=loop, bootstrap_servers=MQ,
    group_id=MQ_GROUP_ID
)
kafka_producer = AIOKafkaProducer(
    loop=loop, bootstrap_servers=MQ, request_timeout_ms=10000,
    connections_max_idle_ms=None
)

CONSUMER = ReconnectingClient(kafka_consumer, "consumer")
PRODUCER = ReconnectingClient(kafka_producer, "producer")

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
        mnm.total.inc()
        try:
            result = await validate(data['url'])
        except ServerDisconnectedError:
            logger.error('Connection to S3 Failed')
            continue

        if 'error' not in result:
            if result['insights_id'] != machine_id:
                response = await post_to_inventory(result, data)
            else:
                response = None

            mnm.valid.inc()
            logger.info("payload_id [%s] validation successful", data['payload_id'])
            data_to_produce = {
                'topic': 'platform.upload.validation',
                'msg': {
                    'id': response.get('id') if response else None,
                    'facts': result,
                    'service': data['service'],
                    'payload_id': data['payload_id'],
                    'account': data['account'],
                    'principal': data['principal'],
                    'b64_identity': data.get('b64_identity'),
                    'validation': 'success'
                }
            }
        else:
            mnm.invalid.inc()
            logger.info("payload_id [%s] validation failed with error: %s", data['payload_id'], result['error'])
            data_to_produce = {
                'topic': 'platform.upload.validation',
                'msg': {
                    'payload_id': data['payload_id'],
                    'validation': 'failure'
                }
            }

        produce_queue.append(data_to_produce)
        logger.info(
            "data for topic [%s], payload_id [%s] put on produce queue (qsize now: %d): %s",
            data_to_produce['topic'], data_to_produce['msg']['payload_id'], len(produce_queue), data_to_produce
        )


def make_producer(queue=None):
    queue = produce_queue if queue is None else queue

    async def send_result(client):
        if not queue:
            await asyncio.sleep(0.1)
        else:
            item = queue.popleft()
            topic, msg, payload_id = item['topic'], item['msg'], item['msg'].get('payload_id')
            logger.info(
                "Popped data from produce queue (qsize now: %d) for topic [%s], payload_id [%s]: %s",
                len(queue), topic, payload_id, msg
            )
            try:
                await client.send_and_wait(topic, json.dumps(msg).encode("utf-8"))
                logger.info("send data for topic [%s] with payload_id [%s] succeeded", topic, payload_id)
            except KafkaError:
                queue.append(item)
                logger.error(
                    "send data for topic [%s] with payload_id [%s] failed, put back on queue (qsize now: %d)",
                    topic, payload_id, len(queue)
                )
                raise
    return send_result


@time(mnm.validation_time)
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
    except (InvalidArchive, ModuleNotFoundError, KeyError) as e:
        facts['error'] = e.args[0]

    return facts


@time(mnm.inventory_post_time)
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
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession() as session:
            async with session.post(INVENTORY_URL, data=json.dumps(post), headers=headers, timeout=timeout) as response:
                if response.status != 200 and response.status != 201:
                    mnm.inventory_post_failure.inc()
                    logger.error(
                        'payload_id [%s] failed to post to inventory: %s', msg['payload_id'], await response.text()
                    )
                else:
                    mnm.inventory_post_success.inc()
                    logger.info("payload_id [%s] posted to inventory", msg['payload_id'])
                    return await response.json()
    except ClientConnectionError as e:
        logger.error("payload_id [%s] failed to post to inventory, unable to connect: %s", msg['payload_id'], e)
        return {"error": "Unable to update inventory. Service unavailable"}


def main():
    try:
        mnm.start_http_server(port=9126)
        loop.set_default_executor(thread_pool_executor)
        loop.create_task(CONSUMER.get_callback(consume)())
        loop.create_task(PRODUCER.get_callback(make_producer(produce_queue))())
        loop.run_forever()
    except KeyboardInterrupt:
        loop.stop()


if __name__ == "__main__":
    date = get_commit_date(BUILD_ID)
    mnm.upload_service_version.info({"version": BUILD_ID, "date": date})
    main()
