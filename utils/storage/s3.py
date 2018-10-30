import boto3
import os
import threading
import time

from botocore.exceptions import ClientError

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', None)
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', None)

# S3 buckets
QUARANTINE = os.getenv('S3_PERM', 'insights-upload-quarantine')
PERM = os.getenv('S3_PERM', 'insights-upload-perm-test')
REJECT = os.getenv('S3_REJECT', 'insights-upload-rejected')

s3 = boto3.client('s3',
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


class UploadProgress(object):
    """Tracks progress of an uploading file."""
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self.percentage = 0.0
        self.time_last_updated = 0.0

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            self.time_last_updated = time.time()
            self.percentage = (self._seen_so_far / self._size) * 100


def write(data, dest, uuid):
    callback = UploadProgress(data)
    s3.upload_file(data, dest, uuid, Callback=callback)
    url = s3.generate_presigned_url('get_object',
                                    Params={'Bucket': dest,
                                            'Key': uuid}, ExpiresIn=100)
    return url, callback


def copy(src, dest, uuid):
    copy_src = {'Bucket': src,
                'Key': uuid}
    s3.copy(copy_src, dest, uuid)
    s3.delete_object(Bucket=src, Key=uuid)
    url = s3.generate_presigned_url('get_object',
                                    Params={'Bucket': dest,
                                            'Key': uuid})
    return url


def ls(src, uuid):
    head_object = s3.head_object(Bucket=src, Key=uuid)
    return head_object


def up_check(name):
    exists = True
    try:
        s3.head_bucket(Bucket=name)
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            exists = False

    return exists
