import logging
import os
import sys
import asyncio
import collections

from logstash_formatter import LogstashFormatterV1
from concurrent.futures import ThreadPoolExecutor
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafkahelpers import ReconnectingClient

from pup.client_context import ClientContext
from pup.utils import mnm, configuration
from pup.utils.get_version_info import get_version_info

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

def main():
    produce_queue = collections.deque([], 999)
    thread_pool_executor = ThreadPoolExecutor(max_workers=configuration.MAX_WORKERS)
    loop = asyncio.get_event_loop()
    client_context = ClientContext(produce_queue, loop)

    kafka_consumer = AIOKafkaConsumer(
        configuration.PUP_QUEUE, loop=loop, bootstrap_servers=configuration.MQ,
        group_id=configuration.MQ_GROUP_ID
    )
    kafka_producer = AIOKafkaProducer(
        loop=loop, bootstrap_servers=configuration.MQ, request_timeout_ms=10000,
        connections_max_idle_ms=None
    )

    reconnecting_consumer = ReconnectingClient(kafka_consumer, "consumer")
    reconnecting_producer = ReconnectingClient(kafka_producer, "producer")

    try:
        mnm.start_http_server(port=9126)
        loop.set_default_executor(thread_pool_executor)
        loop.create_task(reconnecting_consumer.get_callback(client_context.consume)())
        loop.create_task(reconnecting_producer.get_callback(client_context.send_result)())
        loop.run_forever()
    except KeyboardInterrupt:
        loop.stop()


if __name__ == "__main__":
    mnm.upload_service_version.info(get_version_info())
    main()
