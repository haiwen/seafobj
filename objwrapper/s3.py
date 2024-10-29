import boto3
from botocore.exceptions import ClientError

class S3Conf(object):
    def __init__(self, key_id, key, bucket_name, host, port, use_v4_sig, aws_region, use_https, path_style_request, sse_c_key):
        self.key_id = key_id
        self.key = key
        self.bucket_name = bucket_name
        self.host = host
        self.port = port
        self.use_v4_sig = use_v4_sig
        self.aws_region = aws_region
        self.use_https = use_https
        self.path_style_request = path_style_request
        self.sse_c_key = sse_c_key


class SeafS3Client(object):
    """Wraps a s3 connection and a bucket"""
    def __init__(self, conf):
        self.conf = conf
        self.client = None
        self.bucket = None
        self.do_connect()

    def do_connect(self):
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
        addressing_style = 'virtual'
        if self.conf.path_style_request:
            addressing_style = 'path'
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

    def read_obj(self, obj_id):
        if self.conf.sse_c_key:
            obj = self.client.get_object(Bucket=self.bucket, Key=obj_id, SSECustomerKey=self.conf.sse_c_key, SSECustomerAlgorithm='AES256')
        else:
            obj = self.client.get_object(Bucket=self.bucket, Key=obj_id)
        return obj.get('Body').read()

    def get_name(self):
        return 'S3 storage backend'

    def list_objs(self, prefix=None):
        paginator = self.client.get_paginator('list_objects_v2')
        if prefix:
            iterator = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
        else:
            iterator = paginator.paginate(Bucket=self.bucket)
        for page in iterator:
            for content in page.get('Contents', []):
                tokens = content.get('Key', '').split('/', 1)
                if len(tokens) == 2:
                    obj = [tokens[0], tokens[1], content.get('Size', 0)]
                    yield obj


    def obj_exists(self, key):
        bucket = self.bucket
        try:
            if self.conf.sse_c_key:
                self.client.head_object(Bucket=bucket, Key=key, SSECustomerKey=self.conf.sse_c_key, SSECustomerAlgorithm='AES256')
            else:
                self.client.head_object(Bucket=bucket, Key=key)
            exists = True
        except ClientError:
            exists = False

        return exists

    def write_obj(self, data, key, ctime=-1):
        metadata = {}
        if ctime >= 0:
            metadata = {"Ctime":str(ctime)}
        bucket = self.bucket
        if self.conf.sse_c_key:
            self.client.put_object(Bucket=bucket, Key=key, Body=data, Metadata=metadata, SSECustomerKey=self.conf.sse_c_key, SSECustomerAlgorithm='AES256')
        else:
            self.client.put_object(Bucket=bucket, Key=key, Body=data, Metadata=metadata)

    def remove_obj(self, key):
        bucket = self.bucket
        self.client.delete_object(Bucket=bucket, Key=key)

    def stat_obj(self, key):
        bucket = self.bucket
        if self.conf.sse_c_key:
            obj = self.client.head_object(Bucket=bucket, Key=key, SSECustomerKey=self.conf.sse_c_key, SSECustomerAlgorithm='AES256')
        else:
            obj = self.client.head_object(Bucket=bucket, Key=key)
        size = int(obj.get('ContentLength'))
        return size

    def get_ctime(self, key):
        bucket = self.bucket
        if self.conf.sse_c_key:
            obj = self.client.head_object(Bucket=bucket, Key=key, SSECustomerKey=self.conf.sse_c_key, SSECustomerAlgorithm='AES256')
        else:
            obj = self.client.head_object(Bucket=bucket, Key=key)
        metadata = obj.get('Metadata')
        ctime = metadata.get('ctime', '')
        try:
            return float(ctime)
        except:
            return 0
