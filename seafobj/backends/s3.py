import boto3
from botocore.exceptions import ClientError

from .base import AbstractObjStore


class S3Conf(object):
    def __init__(self, key_id, key, bucket_name, host, port, use_v4_sig, aws_region, use_https, path_style_request):
        self.key_id = key_id
        self.key = key
        self.bucket_name = bucket_name
        self.host = host
        self.port = port
        self.use_v4_sig = use_v4_sig
        self.aws_region = aws_region
        self.use_https = use_https
        self.path_style_request = path_style_request


class SeafS3Client(object):
    """Wraps a s3 connection and a bucket"""
    def __init__(self, conf):
        self.conf = conf
        self.client = None
        self.bucket = None

    def do_connect(self):
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
        addressing_style = 'virtual'
        if self.conf.path_style_request:
            addressinng_style = 'path'
        if self.conf.use_v4_sig:
            config = boto3.session.Config(signature_version='s3v4', s3={'addressing_style':addressing_style})
        else:
            config = boto3.session.Config(signature_version='s3',s3={'addressing_style':addressing_style})

        if self.conf.host is None:
            self.client = boto3.client('s3',
                                       region_name=self.conf.aws_region,
                                       aws_access_key_id=self.conf.key_id,
                                       aws_secret_access_key=self.conf.key,
                                       use_ssl=self.conf.use_https,
                                       config=config)
        else:
            # https://github.com/boto/boto3/blob/master/boto3/session.py#L265
            endpoint_url = 'https://%s' % self.conf.host if self.conf.use_https else 'http://%s' % self.conf.host
            if self.conf.port:
                endpoint_url = '%s:%s' % (endpoint_url, self.conf.port)
            self.client = boto3.client('s3',
                                       aws_access_key_id=self.conf.key_id,
                                       aws_secret_access_key=self.conf.key,
                                       endpoint_url=endpoint_url,
                                       config=config)

        self.bucket = self.conf.bucket_name

    def read_object_content(self, obj_id):
        if not self.client or not self.bucket:
            self.do_connect()

        obj = self.client.get_object(Bucket=self.bucket, Key=obj_id)
        return obj.get('Body').read()


class SeafObjStoreS3(AbstractObjStore):
    """S3 backend for seafile objects"""
    def __init__(self, compressed, s3_conf, crypto=None, cache=None):
        AbstractObjStore.__init__(self, compressed, crypto, cache)
        self.s3_client = SeafS3Client(s3_conf)

    def read_obj_raw(self, repo_id, version, obj_id):
        real_obj_id = '%s/%s' % (repo_id, obj_id)
        data = self.s3_client.read_object_content(real_obj_id)
        return data

    def get_name(self):
        return 'S3 storage backend'

    def list_objs(self, repo_id=None):
        if not self.s3_client.client or not self.s3_client.bucket:
            self.s3_client.do_connect()

        start_after = ''
        while True:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html
            if repo_id:
                objects = self.s3_client.client.list_objects_v2(Bucket=self.s3_client.bucket, StartAfter=start_after,
                                                                Prefix=repo_id)
            else:
                objects = self.s3_client.client.list_objects_v2(Bucket=self.s3_client.bucket, StartAfter=start_after)

            if len(objects.get('Contents', [])) == 0:
                break

            for content in objects.get('Contents', []):
                tokens = content.get('Key', '').split('/')
                if len(tokens) == 2:
                    repo_id = tokens[0]
                    obj_id = tokens[1]
                    obj = [repo_id, obj_id, content.get('Size', 0)]
                    yield obj

            # The 'Contents' of response is a list, each element is a dict,
            # and each dict must contain the 'Key'.
            # Use the 'Key' of the last dict as the 'StartAfter' parameter of the next list_objects_v2().
            # If the dict does not contain 'Key', terminate the loop,
            # otherwise will fall into an infinite loop
            start_after = objects.get('Contents', [])[-1].get('Key', '')
            if not objects.get('IsTruncated', False) or not start_after:
                break

    def obj_exists(self, repo_id, obj_id):
        if not self.s3_client.client or not self.s3_client.bucket:
            self.s3_client.do_connect()

        bucket = self.s3_client.bucket
        s3_path = '%s/%s' % (repo_id, obj_id)
        try:
            self.s3_client.client.head_object(Bucket=bucket, Key=s3_path)
            exists = True
        except ClientError:
            exists = False

        return exists

    def write_obj(self, data, repo_id, obj_id):
        if not self.s3_client.client or not self.s3_client.bucket:
            self.s3_client.do_connect()

        bucket = self.s3_client.bucket
        s3_path = '%s/%s' % (repo_id, obj_id)
        self.s3_client.client.put_object(Bucket=bucket, Key=s3_path, Body=data)

    def remove_obj(self, repo_id, obj_id):
        if not self.s3_client.client or not self.s3_client.bucket:
            self.s3_client.do_connect()

        bucket = self.s3_client.bucket
        s3_path = '%s/%s' % (repo_id, obj_id)
        self.s3_client.client.delete_object(Bucket=bucket, Key=s3_path)

    def stat_raw(self, repo_id, obj_id):
        if not self.s3_client.client or not self.s3_client.bucket:
            self.s3_client.do_connect()

        bucket = self.s3_client.bucket
        s3_path = '%s/%s' % (repo_id, obj_id)
        obj = self.s3_client.client.get_object(Bucket=bucket, Key=s3_path)
        size = int(obj.get('ContentLength'))
        return size
