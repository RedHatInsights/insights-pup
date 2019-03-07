import logging
import asyncio
import json

from kafka.errors import KafkaError

from pup.utils import configuration
from pup.consumer import handle_file


logger = logging.getLogger('advisor-pup')


class ClientContext():
    """
    This class exists to keep shared state in `produce_queue` between the
    consume and send_result methods. It also has the coroutines for the PUP
    consumer and producer.
    """

    def __init__(self, produce_queue, loop):
        # local queue for pushing items into kafka, this queue fills up if kafka
        # goes down
        self.produce_queue = produce_queue
        self.loop = loop

    async def consume(self, client):
        data = await client.getmany()
        for tp, msgs in data.items():
            if tp.topic == configuration.PUP_QUEUE:
                logger.info("received messages: %s", msgs)
                self.loop.create_task(handle_file(msgs, self.produce_queue))
        await asyncio.sleep(0.1)

    async def send_result(self, client):
        if not self.produce_queue:
            await asyncio.sleep(0.1)
        else:
            item = self.produce_queue.popleft()
            topic, msg, payload_id = item['topic'], item['msg'], item['msg'].get('payload_id')
            logger.info(
                "Popped data from produce queue (qsize now: %d) for topic [%s], payload_id [%s]: %s",
                len(self.produce_queue), topic, payload_id, msg
            )
            try:
                await client.send_and_wait(topic, json.dumps(msg).encode("utf-8"))
                logger.info("send data for topic [%s] with payload_id [%s] succeeded", topic, payload_id)
            except KafkaError:
                self.produce_queue.append(item)
                logger.error(
                    "send data for topic [%s] with payload_id [%s] failed, put back on queue (qsize now: %d)",
                    topic, payload_id, len(self.produce_queue)
                )
                raise
