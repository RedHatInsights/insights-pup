import os

# Maximum workers for threaded execution
BUILD_ID = os.getenv('OPENSHIFT_BUILD_COMMIT', "unknown")
DEVMODE = os.getenv('DEVMODE', False)
INVENTORY_URL = os.getenv('INVENTORY_URL', 'http://inventory:5000/api/hosts')
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 50))
MQ_GROUP_ID = os.getenv('MQ_GROUP_ID', 'advisor-pup')
MQ = os.getenv('KAFKAMQ', 'kafka:29092').split(',')
PUP_QUEUE = os.getenv('PUP_QUEUE', 'platform.upload.pup')
AWS_ACCESS_KEY_ID = os.getenv('CW_AWS_ACCESS_KEY_ID', None)
AWS_SECRET_ACCESS_KEY = os.getenv('CW_AWS_SECRET_ACCESS_KEY', None)
AWS_REGION_NAME = os.getenv('CW_AWS_REGION_NAME', 'us-east-1')
SYSTEM_PROFILE_QUEUE = os.getenv("SYSTEM_PROFILE_QUEUE", "platform.system-profile")
MAX_RECORDS = int(os.getenv("MAX_RECORDS", 1))
FACT_EXTRACT_LOGLEVEL = os.getenv("FACT_EXTRACT_LOGLEVEL", "ERROR")
LOG_GROUP = os.getenv("LOG_GROUP", "platform")
