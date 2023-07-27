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
        if self.conf.use_v4_sig:
            config = boto3.session.Config(signature_version='s3v4')
        else:
            config = boto3.session.Config(signature_version='s3')

        if self.conf.host is None:
            self.client = boto3.client('s3',
                                       region_name=self.conf.aws_region,
                                       aws_access_key_id=self.conf.key_id,
                                       aws_secret_access_key=self.conf.key,
                                       use_ssl=self.conf.use_https,
                                       config=config)
        else:
            # https://github.com/boto/boto3/blob/1.28.12/boto3/session.py#L265
            endpoint_url = 'https://%s' % self.conf.host if self.conf.use_https else 'http://%s' % self.conf.host
            self.client = boto3.client('s3',
                                       region_name=self.conf.aws_region,
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
    def __init__(self, compressed, s3_conf, crypto=None):
        AbstractObjStore.__init__(self, compressed, crypto)
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

        if repo_id:
            keys = self.s3_client.client.list_objects(Bucket=self.s3_client.bucket, Prefix=repo_id)
        else:
            keys = self.s3_client.client.list_objects(Bucket=self.s3_client.bucket)

        for key in keys:
            tokens = key.get('Key', '').split('/')
            if len(tokens) == 2:
                _repo_id = tokens[0]
                obj_id = tokens[1]
                obj = [_repo_id, obj_id, 0]
                yield obj

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
