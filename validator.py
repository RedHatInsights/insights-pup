import logging
import os
import json
import base64
import aiohttp
import tornado
import tornado.web
import sys

from tornado.ioloop import IOLoop
from tempfile import NamedTemporaryFile

from logstash_formatter import LogstashFormatterV1
from concurrent.futures import ThreadPoolExecutor
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
        format="%(asctime)s %(threadName)s %(levelname)s -- %(message)s"
    )
logger = logging.getLogger('validator')

# Maximum workers for threaded execution
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 50))

LISTEN_PORT = int(os.getenv('LISTEN_PORT', 8080))

# env variable to tell which files to grab from an archive
CANONICAL_FACTS = {
    'insights-id': Specs.machine_id,
    'fqdn': Specs.hostname
}

INVENTORY_URL = os.getenv('INVENTORY_URL', 'http://inventory:5000/api/hosts')

thread_pool_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)


async def extract_facts(archive):
    facts = {}
    with extract(archive) as ex:
        broker = run(root=ex.tmp_dir)
        for k, v in CANONICAL_FACTS.items():
            facts[k] = ('\n'.join(broker[v].content))

    return facts


session = aiohttp.ClientSession()


class ValidateHandler(tornado.web.RequestHandler):

    async def post_to_inventory(self, facts, data):

        post = {**facts, **data}
        post['account'] = post.pop('rh_account')
        post['canonical_facts'] = {}

        identity = base64.b64encode(json.dumps({'identity': {'account_number': post['account'], 'org_id': data['principal']}}).encode()).decode()
        headers = {'x-rh-identity': identity,
                   'Content-Type': 'application/json'}

        async with session.post(INVENTORY_URL, data=json.dumps(post), headers=headers) as response:
            if response.status != 200:
                logger.error('Failed to post to inventory: ' + await response.text())
            else:
                logger.info("payload posted to inventory: %s", data['payload_id'])
                return await response.json()

    async def validate(self, url):

        temp = NamedTemporaryFile(delete=False).name

        async with session.get(url) as response:
            open(temp, 'wb').write(await response.read())

        try:
            return await extract_facts(temp)
        finally:
            os.remove(temp)

    async def post(self):

        data = json.loads(self.request.body)

        result = {
            "validation": "success",
            "payload_id": data['payload_id'],
        }

        try:
            facts = await self.validate(data['url'])
        except Exception:
            result["validation"] = "failure"

        if facts:
            inv = await self.post_to_inventory(facts, data)
            result["id"] = inv['id']
        else:
            result["validation"] = "failure"

        self.write(result)
        self.set_status(202)


class RootHandler(tornado.web.RequestHandler):
    """Handles requests to root. Useful for health checks"""

    def get(self):
        self.write("lub-dub")


endpoints = [
    (r"/", RootHandler),
    (r"/api/validate", ValidateHandler)
]

app = tornado.web.Application(endpoints)


def main():
    app.listen(LISTEN_PORT)
    logger.info(f"Web server listening on port {LISTEN_PORT}")
    loop = IOLoop.current()
    loop.set_default_executor(thread_pool_executor)
    try:
        loop.start()
    except KeyboardInterrupt:
        loop.stop()


if __name__ == "__main__":
    main()
