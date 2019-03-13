import os

# Maximum workers for threaded execution
BUILD_ID = os.getenv('OPENSHIFT_BUILD_COMMIT')
DEVMODE = os.getenv('DEVMODE', False)
INVENTORY_URL = os.getenv('INVENTORY_URL', 'http://inventory:5000/api/hosts')
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 50))
MQ_GROUP_ID = os.getenv('MQ_GROUP_ID', 'advisor-pup')
MQ = os.getenv('KAFKAMQ', 'kafka:29092').split(',')
PUP_QUEUE = os.getenv('PUP_QUEUE', 'platform.upload.pup')
