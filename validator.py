import logging
import asyncio
import collections
import tarfile
import requests
import os
import json

from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.errors import KafkaError
from argparse import Namespace


# Logging
logging.basicConfig(
    level=os.getenv("LOGLEVEL", "INFO"),
    format="%(asctime)s %(threadName)s %(levelname)s -- %(message)s"
)
logger = logging.getLogger('validator')

# Where to pull and push payloads to (s3, azure, local, etc.)
storage_driver = os.getenv("STORAGE_DRIVER", "s3")
storage = import_module("utils.storage.{}".format(storage_driver))

# Maximum workers for threaded execution
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 50))

# env variable to tell which files to grab from an archive
FILEPATHS = os.getenv('FILEPATHS').split(',')


loop = asyncio.get_event_loop()
thread_pool_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

# Message Queue
MQ = os.getenv('MQ_URL', 'kafka:29092').split(',')
MQ_GROUP_ID = os.getenv('MQ_GROUP_ID', 'validator')
mqc = AIOKafkaConsumer(
    'platform.upload.validator', loop=loop, bootstrap_servers=MQ,
    group_id=MQ_GROUP_ID
)

mqp = AIOKafkaProducer(
    loop=loop, bootstrap_servers=MQ, request_timeout_ms=10000,
    connections_max_idle_ms=None
)

# local queue for pushing items into kafka, this queue fills up if kafka goes down
produce_queue = collections.deque([], 999)


async def check_tar(archive, payload_id):
    if not tarfile.is_tarfile(archive):
        logger.error('Payload %s is not a tar file.', payload_id)
        return

    try:
        tar = tarfile.open(archive)
    except tarfile.ReadError:
        logger.error('Payload %s cannot be opened', payload_id)
        return

    # check uncompressed size
    size = 0
    for name in tar:
        size += name.size

    yield True


# Pull facts out of the archive to place in the message
async def extract_facts(archive, payload_id):
    facts = {}
    tar = tarfile.open(archive)
    # this currently assumes the root is the insights archive name
    root = tar.getnames()[1]
    for path in FILEPATHS:
        content = tar.extractfile(root + path)
        facts[path] = content.read()

    yield facts


class MQStatus(object):
    """Class used to track the status of the producer/consumer clients."""
    mqc_connected = False
    mqp_connected = False


async def consumer(loop=loop):
    """Consume indefinitely from the validator queue."""
    MQStatus.mqc_connected = False
    while True:
        # If not connected, attempt to connect...
        if not MQStatus.mqc_connected:
            try:
                logger.info("Consume client not connected, attempting to connect...")
                await mqc.start()
                logger.info("Consumer client connected!")
                MQStatus.mqc_connected = True
            except KafkaError:
                logger.exception('Consume client hit error, triggering re-connect...')
                await asyncio.sleep(5)
                continue

        # Consume
        try:
            data = await mqc.getmany()
            for tp, msgs in data.items():
                if tp.topic == 'platform.upload.validator':
                    await handle_file(msgs)
        except KafkaError:
            logger.exception('Consume client hit error, triggering re-connect...')
            MQStatus.mqc_connected = False
        await asyncio.sleep(0.1)


async def producer(loop=loop):
    """boop
    """
    MQStatus.mqp_connected = False
    while True:
        # If not connected to kafka, attempt to connect...
        if not MQStatus.mqp_connected:
            try:
                logger.info("Producer client not connected, attempting to connect...")
                await mqp.start()
                logger.info("Producer client connected!")
                MQStatus.mqp_connected = True
            except KafkaError:
                logger.exception('Producer client hit error, triggering re-connect...')
                await asyncio.sleep(5)
                continue

        # Pull items off our queue to produce
        if not produce_queue:
            await asyncio.sleep(0.1)
            continue

        for _ in range(0, len(produce_queue)):
            item = produce_queue.popleft()
            topic = item['topic']
            msg = item['msg']
            logger.info(
                "Popped item from produce queue (qsize: %d): topic %s: %s",
                len(produce_queue), topic, msg
            )
            try:
                await mqp.send_and_wait(topic, json.dumps(msg).encode('utf-8'))
                logger.info("Produced on topic %s: %s", topic, msg)
            except KafkaError:
                logger.exception('Producer client hit error, triggering re-connect...')
                MQStatus.mqp_connected = False
                # Put the item back on the queue so we can push it when we reconnect
                produce_queue.appendleft(item)


async def handle_file(msgs):
    """
    """
    for msg in msgs:
        try:
            data = json.loads(msg.value)
        except ValueError:
            logger.error("handle_file(): unable to decode msg as json: {}".format(msg.value))
            continue

        if 'payload_id' not in data:
            logger.error("payload_id not in message. Payload not removed from quarantine.")
            return

        received = Namespace(**data)
        tarfile = requests.get(received.pop('url'))

        tar_check = await check_tar(tarfile)

        if tar_check:
            facts = await extract_facts(tarfile, received['payload_id'])

            url = await loop.run_in_executor(
                None, storage.copy, storage.QUARANTINE, storage.PERM, received['payload_id']
            )
            logger.info(url)
            received['url'] = url

            publish = {**facts, **received}

            if publish:
                produce_queue.append({'topic': 'platform.validator.result', 'msg': publish})
                logger.info(
                    "Data for payload_id [%s] put on produce queue (qsize: %d)",
                    data['payload_id'], len(produce_queue)
                )
        else:
            url = await loop.run_in_executor(
                None, storage.copy, storage.QUARANTINE, storage.REJECT, received['payload_id']
            )
            logger.error('Payload failed tests. Rejected ID %s', received['payload_id'])


def main():
    try:
        loop.set_default_executor(thread_pool_executor)
        loop.run_until_complete(consumer())
        loop.run_until_complete(producer())
    except KeyboardInterrupt:
        loop.stop()


if __name__ == "__main__":
    main()
