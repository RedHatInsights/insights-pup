import logging
import os
import sys
import asyncio
import collections
import json
import aiohttp
import watchtower
import signal

from aiohttp.client_exceptions import ClientConnectionError
from logstash_formatter import LogstashFormatterV1
from concurrent.futures import ThreadPoolExecutor
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.errors import KafkaError
from kafkahelpers import ReconnectingClient, make_producer
from prometheus_async.aio import time
from boto3.session import Session

from pup.utils import mnm, configuration
from pup.utils.fact_extract import extract_facts
from pup.utils.get_commit_date import get_commit_date

TASK_LOOPS = {}

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
try:
    with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace', 'r') as f:
        NAMESPACE = f.read()
except EnvironmentError:
    logger.info('Not Running on Openshift')

if (configuration.AWS_ACCESS_KEY_ID and configuration.AWS_SECRET_ACCESS_KEY):
    CW_SESSION = Session(aws_access_key_id=configuration.AWS_ACCESS_KEY_ID,
                         aws_secret_access_key=configuration.AWS_SECRET_ACCESS_KEY,
                         region_name=configuration.AWS_REGION_NAME)
    cw_handler = watchtower.CloudWatchLogHandler(boto3_session=CW_SESSION,
                                                 log_group='platform',
                                                 stream_name=NAMESPACE)
    cw_handler.setFormatter(LogstashFormatterV1())
    logger.addHandler(cw_handler)

thread_pool_executor = ThreadPoolExecutor(max_workers=configuration.MAX_WORKERS)
fact_extraction_executor = ThreadPoolExecutor(1)
loop = asyncio.get_event_loop()

kafka_consumer = AIOKafkaConsumer(
    configuration.PUP_QUEUE, loop=loop, bootstrap_servers=configuration.MQ,
    group_id=configuration.MQ_GROUP_ID
)
kafka_producer = AIOKafkaProducer(
    loop=loop, bootstrap_servers=configuration.MQ, request_timeout_ms=10000,
    connections_max_idle_ms=None
)
system_profile_producer = AIOKafkaProducer(
    loop=loop, bootstrap_servers=configuration.MQ, request_timeout_ms=10000,
    connections_max_idle_ms=None
)

CONSUMER = ReconnectingClient(kafka_consumer, "consumer")
PRODUCER = ReconnectingClient(kafka_producer, "producer")
SYSTEM_PROFILE_PRODUCER = ReconnectingClient(system_profile_producer, "system-profile-producer")

# local queue for pushing items into kafka, this queue fills up if kafka goes down
produce_queue = collections.deque()
mnm.produce_queue_size.set_function(lambda: len(produce_queue))
system_profile_queue = collections.deque()
mnm.system_profile_queue_size.set_function(lambda: len(system_profile_queue))
current_archives = []
mnm.current_archives_size.set_function(lambda: len(current_archives))


def get_extra(account="unknown", request_id="unknown"):
    """Add extra indexable fields for logging.

    Keyword Arguments:
        account {str} -- The account number for the upload being processed (default: {"unknown"})
        request_id {str} -- The request ID for the upload being processed (default: {"unknown"})

    Returns:
        dict -- dictionary of extra items
    """
    extra = {"account": account,
             "request_id": request_id
             }
    return extra


async def consume(client):
    data = await client.getmany(timeout_ms=1000, max_records=configuration.MAX_RECORDS)
    for tp, msgs in data.items():
        logger.info("received messages: %s", msgs)
        loop.create_task(handle_file(msgs))
    await asyncio.sleep(0.1)


def fail_upload(data, response, extra):
    mnm.invalid.inc()
    logger.info("payload_id [%s] validation failed with error: %s", data['payload_id'], response['error'], extra=extra)
    data_to_produce = {
        'topic': 'platform.upload.validation',
        'msg': {
            'payload_id': data['payload_id'],
            'validation': 'failure'
        }
    }
    return data_to_produce


def succeed_upload(data, response, extra):
    mnm.valid.inc()
    logger.info("payload_id [%s] validation successful", data['payload_id'], extra=extra)
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


@time(mnm.handle_file_time)
async def handle_file(msgs):
    extra = get_extra()
    for msg in msgs:
        try:
            data = json.loads(msg.value)
            extra["account"] = data["account"]
            extra["request_id"] = data["payload_id"]
        except Exception:
            logger.exception("handle_file(): unable to decode msg as json: %s", msg.value)
            continue

        mnm.total.inc()
        try:
            current_archives.append(data["payload_id"])
            result = await validate(data['url'], data["payload_id"], data["account"])
        except Exception as e:
            logger.exception("Validation encountered error: %s", e, extra=extra)
            continue

        if result is None:
            logger.info("Validation resulted in a None...ignoring msg", extra=extra)
            continue

        # we do not want to POST the system profile to inventory
        # until after we get an id
        system_profile = result.pop("system_profile")

        data["satellite_managed"] = system_profile.get("satellite_managed")

        if len(result) > 0 and 'error' not in result:
            if not data.get('id'):
                logger.info("Inventory ID not included in message from upload-service [%s]", data["payload_id"], extra=extra)
                response = await post_to_inventory(result, data)
            else:
                logger.info("Not posting to inventory, using ID from upload-service (%s)", data.get("id"), extra=extra)
                response = {"id": data.get('id')}

            if response.get('error'):
                data_to_produce = fail_upload(data, response, extra)
            else:
                # As long as we get an id back from inventory
                # we can send the system profile
                system_profile_queue.append({
                    "id": response["id"],
                    "account": data["account"],
                    "request_id": data["payload_id"],
                    "system_profile": system_profile
                })
                data_to_produce = succeed_upload(data, response, extra)

        else:
            data_to_produce = fail_upload(data, result, extra)

        produce_queue.append(data_to_produce)
        logger.info(
            "data for topic [%s], payload_id [%s] put on produce queue (qsize now: %d): %s",
            data_to_produce['topic'], data_to_produce['msg']['payload_id'], len(produce_queue), data_to_produce,
            extra=extra)


def make_responder(queue=None):
    extra = get_extra()
    queue = produce_queue if queue is None else queue

    async def send_result(client):
        if not queue:
            await asyncio.sleep(0.1)
        else:
            item = queue.popleft()
            topic, msg, payload_id = item['topic'], item['msg'], item['msg'].get('payload_id')
            extra["account"] = msg["account"]
            extra["request_id"] = payload_id
            logger.info(
                "Popped data from produce queue (qsize now: %d) for topic [%s], payload_id [%s]",
                len(queue), topic, payload_id, extra=extra)
            try:
                await client.send_and_wait(topic, json.dumps(msg).encode("utf-8"))
                current_archives.remove(payload_id)
                logger.info("send data for topic [%s] with payload_id [%s] succeeded", topic, payload_id, extra=extra)
            except KafkaError:
                queue.append(item)
                logger.error(
                    "send data for topic [%s] with payload_id [%s] failed, put back on queue (qsize now: %d)",
                    topic, payload_id, len(queue), extra=extra)
                raise
    return send_result


async def send_system_profile(client, item):
    extra = get_extra(item["account"], item["request_id"])

    await client.send_and_wait(
        configuration.SYSTEM_PROFILE_QUEUE,
        json.dumps(item).encode("utf-8")
    )
    logger.info("System profile sent for inventory id %s.", item["id"], extra=extra)


@time(mnm.validation_time)
async def validate(url, request_id, account):
    extra = get_extra(account, request_id)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.read()
                mnm.payload_size.observe(len(data))
                return await loop.run_in_executor(fact_extraction_executor, extract_facts, data, request_id, account, extra)
    except Exception as e:
        logger.exception("Validation failure: %s", e, extra=extra)


@time(mnm.inventory_post_time)
async def post_to_inventory(facts, msg):
    extra = get_extra()
    post = {**facts, **msg}
    post['account'] = post.pop('rh_account')
    post['facts'] = []
    if post.get('metadata'):
        post['facts'].append({'facts': post.pop('metadata'),
                              'namespace': 'insights-client'})

    headers = {'x-rh-identity': post['b64_identity'],
               'Content-Type': 'application/json',
               'x-rh-insights-request-id': post['payload_id']}

    extra["account"] = post["account"]
    extra["request_id"] = post["payload_id"]
    try:
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession() as session:
            async with session.post(configuration.INVENTORY_URL, data=json.dumps([post]), headers=headers, timeout=timeout) as response:
                response_json = await response.json()
                if response.status != 207:
                    error = response_json.get('detail')
                    logger.error('Failed to post to inventory: %s', error, extra=extra)
                    return {"error": "Failed to post to inventory."}
                elif response_json['data'][0]['status'] != 200 and response_json['data'][0]['status'] != 201:
                    mnm.inventory_post_failure.inc()
                    logger.error(
                        'payload_id [%s] failed to post to inventory.', msg['payload_id'], extra=extra
                    )
                    logger.error(
                        'inventory error response: %s', await response.text(), extra=extra
                    )
                    return {"error": "Failed to post to inventory."}
                else:
                    mnm.inventory_post_success.inc()
                    logger.info("payload_id [%s] posted to inventory: ID [%s]",
                                msg['payload_id'],
                                response_json['data'][0]['host']['id'],
                                extra={"request_id": post['payload_id'],
                                       "account": post["account"]})
                    return response_json['data'][0]['host']
            await session.close()

    except ClientConnectionError as e:
        logger.error("payload_id [%s] failed to post to inventory, unable to connect: %s", msg['payload_id'], e,
                     extra=extra)
        return {"error": "Unable to update inventory. Service unavailable"}


async def shutdown(signal, loop):
    logger.error("Recieved Exit Signal: %s", signal.name)
    logger.debug("Cancelling Consumer ...")
    TASK_LOOPS["consumer"].cancel()
    while len(current_archives) > 0:
        logger.debug("Remaining Archives: %s", len(current_archives))
        await asyncio.sleep(1)

    logger.debug("Cancelling Producers ... ")
    TASK_LOOPS["producer"].cancel()
    TASK_LOOPS["sysprofile_producer"].cancel()

    loop.stop()
    logger.debug("Shutting Down Thread Pools")
    fact_extraction_executor.shutdown(wait=True)
    thread_pool_executor.shutdown(wait=True)
    logger.info("PUP Shutdown")
    logging.shutdown()


def main():
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: loop.create_task(shutdown(s, loop)))

    mnm.start_http_server(port=9126)
    loop.set_default_executor(thread_pool_executor)
    TASK_LOOPS["consumer"] = loop.create_task(CONSUMER.get_callback(consume)())
    TASK_LOOPS["producer"] = loop.create_task(PRODUCER.get_callback(make_responder(produce_queue))())
    TASK_LOOPS["sysprofile_producer"] = loop.create_task(SYSTEM_PROFILE_PRODUCER.get_callback(make_producer(send_system_profile, system_profile_queue))())
    logger.info("PUP Service Activated")
    loop.run_forever()


if __name__ == "__main__":
    if configuration.DEVMODE:
        date = 'devmode'
    else:
        date = get_commit_date(configuration.BUILD_ID)
    mnm.upload_service_version.info({"version": configuration.BUILD_ID, "date": date})
    main()
