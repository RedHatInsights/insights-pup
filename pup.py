import logging
import os
import sys
import asyncio
import collections
import json
import aiohttp
import watchtower

from tempfile import NamedTemporaryFile
from aiohttp.client_exceptions import ClientConnectionError, ServerDisconnectedError
from logstash_formatter import LogstashFormatterV1
from concurrent.futures import ThreadPoolExecutor
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.errors import KafkaError
from kafkahelpers import ReconnectingClient
from prometheus_async.aio import time
from boto3.session import Session

from pup.utils import mnm, configuration
from pup.utils.fact_extract import extract_facts
from pup.utils.get_commit_date import get_commit_date

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
if (configuration.AWS_ACCESS_KEY_ID and configuration.AWS_SECRET_ACCESS_KEY):
    CW_SESSION = Session(aws_access_key_id=configuration.AWS_ACCESS_KEY_ID,
                         aws_secret_access_key=configuration.AWS_SECRET_ACCESS_KEY,
                         region_name=configuration.AWS_REGION_NAME)
    logger.addHandler(watchtower.CloudWatchLogHandler(boto3_session=CW_SESSION))

thread_pool_executor = ThreadPoolExecutor(max_workers=configuration.MAX_WORKERS)
loop = asyncio.get_event_loop()

kafka_consumer = AIOKafkaConsumer(
    configuration.PUP_QUEUE, loop=loop, bootstrap_servers=configuration.MQ,
    group_id=configuration.MQ_GROUP_ID
)
kafka_producer = AIOKafkaProducer(
    loop=loop, bootstrap_servers=configuration.MQ, request_timeout_ms=10000,
    connections_max_idle_ms=None
)

CONSUMER = ReconnectingClient(kafka_consumer, "consumer")
PRODUCER = ReconnectingClient(kafka_producer, "producer")

# local queue for pushing items into kafka, this queue fills up if kafka goes down
produce_queue = collections.deque([], 999)


async def consume(client):
    data = await client.getmany()
    for tp, msgs in data.items():
        if tp.topic == configuration.PUP_QUEUE:
            logger.info("received messages: %s", msgs)
            loop.create_task(handle_file(msgs))
    await asyncio.sleep(0.1)


def fail_upload(data, response):
    mnm.invalid.inc()
    logger.info("payload_id [%s] validation failed with error: %s", data['payload_id'], response['error'])
    data_to_produce = {
        'topic': 'platform.upload.validation',
        'msg': {
            'payload_id': data['payload_id'],
            'validation': 'failure'
        }
    }
    return data_to_produce


def succeed_upload(data, response):
    mnm.valid.inc()
    logger.info("payload_id [%s] validation successful", data['payload_id'])
    data_to_produce = {
        'topic': 'platform.upload.validation',
        'msg': {
            'id': response.get('id') if response else None,
            'service': data['service'],
            'payload_id': data['payload_id'],
            'account': data['account'],
            'principal': data['principal'],
            'b64_identity': data.get('b64_identity'),
            'satellite_managed': data.get('satellite_managed'),
            'validation': 'success'
        }
    }
    return data_to_produce


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
        except (ServerDisconnectedError, ClientConnectionError):
            logger.error('Connection to S3 Failed')
            continue

        data["satellite_managed"] = result.get("system_profile").get("satellite_managed")

        if len(result) > 0 and 'error' not in result:
            if result.get('insights_id') != machine_id:
                response = await post_to_inventory(result, data)
            else:
                response = {"id": result.get('insights_id')}

            if response.get('error'):
                data_to_produce = fail_upload(data, response)
            else:
                data_to_produce = succeed_upload(data, response)

        else:
            data_to_produce = fail_upload(data, result)

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

    def _write(filename, data):
        with open(filename, "wb") as f:
            f.write(data)

    try:
        temp = NamedTemporaryFile(delete=False).name

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.read()
                await loop.run_in_executor(None, _write, temp, data)
            await session.close()

        return await loop.run_in_executor(None, extract_facts, temp)
    finally:
        os.remove(temp)


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
            async with session.post(configuration.INVENTORY_URL, data=json.dumps([post]), headers=headers, timeout=timeout) as response:
                response_json = await response.json()
                if response.status != 207:
                    error = response_json.get('detail')
                    logger.error('Failed to post to inventory: %s', error)
                    return {"error": "Failed to post to inventory."}
                elif response_json['data'][0]['status'] != 200 and response_json['data'][0]['status'] != 201:
                    mnm.inventory_post_failure.inc()
                    logger.error(
                        'payload_id [%s] failed to post to inventory.', msg['payload_id']
                    )
                    logger.error(
                        'inventory error response: %s', await response.text()
                    )
                    return {"error": "Failed to post to inventory."}
                else:
                    mnm.inventory_post_success.inc()
                    logger.info("payload_id [%s] posted to inventory: ID [%s]",
                                msg['payload_id'],
                                response_json['data'][0]['host']['id'])
                    return response_json['data'][0]['host']
            await session.close()

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
    if configuration.DEVMODE:
        date = 'devmode'
    else:
        date = get_commit_date(configuration.BUILD_ID)
    mnm.upload_service_version.info({"version": configuration.BUILD_ID, "date": date})
    main()
