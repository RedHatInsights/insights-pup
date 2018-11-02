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
from tempfile import NamedTemporaryFile


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
        yield False
    logger.info('%s is a tar', payload_id)

    try:
        tar = tarfile.open(archive)
    except tarfile.ReadError:
        logger.error('Payload %s cannot be opened', payload_id)
        yield False

    # check uncompressed size
    size = 0
    for name in tar:
        size += name.size

    yield True


# Pull facts out of the archive to place in the message
async def extract_facts(archive):
    facts = {}
    try:
        tar = tarfile.open(archive)
    except ValueError:
        logger.error('Tarfile not found')
        return
    # this currently assumes the root is the insights archive name
    root = tar.getnames()[1]
    for path in FILEPATHS:
        if root + path in tar.getnames():
            content = tar.extractfile(root + path)
            facts[path] = content.read().decode('utf-8')
    return facts


mq_conn_status = {"consumer": False, "producer": False}


async def ensure_connected(direction, level=0):
    if level > 5:
        raise KafkaError("Failed to connect after 5 attempts")

    if not mq_conn_status[direction]:
        try:
            logger.info(f"{direction} client not connected, attempting to connect...")
            await mqc.start() if direction == "consumer" else mqp.start()            
            logger.info(f"{direction} client connected!")
            mq_conn_status[direction] = True
        except KafkaError:
            logger.exception(f"{direction} client hit error, triggering re-connect...")
            await asyncio.sleep(5)
            await ensure_connected(direction, level=level + 1)


async def consumer(loop=loop):
    """Consume indefinitely from the validator queue."""
    mq_conn_status["consumer"] = False
    while True:
        await ensure_connected("consumer")

        # Consume
        try:
            data = await mqc.getmany()
            for tp, msgs in data.items():
                if tp.topic == 'platform.upload.validator':
                    await handle_file(msgs)
        except KafkaError:
            logger.exception("Consume client hit error, triggering re-connect...")
            mq_conn_status["consumer"] = False

        await asyncio.sleep(0.1)


async def producer(loop=loop):
    mq_conn_status["producer"] = False
    while True:
        await ensure_connected("producer")

        for topic, msg in produce_queue:
            logger.info(
                "Popped item from produce queue (qsize: %d): topic %s: %s",
                len(produce_queue), topic, msg
            )

            try:
                await mqp.send_and_wait(topic, json.dumps(msg).encode('utf-8'))
                logger.info("Produced on topic %s: %s", topic, msg)
            except KafkaError:
                logger.exception("Producer client hit error, triggering re-connect...")
                mq_conn_status["producer"] = False
                # Put the item back on the queue so we can push it when we reconnect
                produce_queue.appendleft((topic, msg))
                break

        await asyncio.sleep(0.1)


async def handle_file(msgs):
    for msg in msgs:
        try:
            data = json.loads(msg.value)
        except ValueError:
            logger.error("handle_file(): unable to decode msg as json: {}".format(msg.value))
            continue

        if 'payload_id' not in data:
            logger.error("payload_id not in message. Payload not removed from quarantine.")
            return

        if not data['url']:
            logger.error('No URL in message. Cannot download payload')
            return

        tar_file = requests.get(data.pop('url'))

        if tar_file.status_code != 200:
            return

        tempfile = NamedTemporaryFile(delete=False).name

        try:
            open(tempfile, 'wb').write(tar_file.content)

            if check_tar(tempfile, data['payload_id']):
                facts = await extract_facts(tempfile)

                url = await loop.run_in_executor(
                    None, storage.copy, storage.QUARANTINE, storage.PERM, data['payload_id']
                )
                data['url'] = url

                publish = {**facts, **data}
                if publish:
                    produce_queue.append(('platform.upload.result', publish))
                    logger.info(
                        "Data for payload_id [%s] put on produce queue (qsize: %d)",
                        data['payload_id'], len(produce_queue)
                    )
            else:
                url = await loop.run_in_executor(
                    None, storage.copy, storage.QUARANTINE, storage.REJECT, data['payload_id']
                )
                logger.error('Payload failed tests. Rejected ID %s', data['payload_id'])
        finally:
            os.remove(tempfile)


def main():
    try:
        loop.set_default_executor(thread_pool_executor)
        loop.create_task(consumer())
        loop.create_task(producer())
        loop.run_forever()
    except KeyboardInterrupt:
        loop.stop()


if __name__ == "__main__":
    main()
