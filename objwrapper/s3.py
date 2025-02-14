import boto3
from botocore.exceptions import ClientError
from objwrapper.exceptions import InvalidConfigError
import requests
from datetime import datetime
import hmac
import hashlib
import base64
from lxml import etree

class S3Conf(object):
    def __init__(self, key_id, key, bucket_name, host, port, use_v4_sig, aws_region, use_https, path_style_request, sse_c_key):
        if not host and not aws_region:
            raise InvalidConfigError('aws_region and host are not configured')
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
        self.enpoint_url = None;
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
            if self.conf.use_https:
                self.endpoint_url = f'https://s3.{self.conf.aws_region}.amazonaws.com'
            else:
                self.endpoint_url = f'http://s3.{self.conf.aws_region}.amazonaws.com'
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
            self.endpoint_url = endpoint_url
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
        if not self.conf.use_v4_sig and self.conf.path_style_request:
            # When using the S3 v2 protocol and path-style requests, boto3 is unable to list objects properly.
            # We manually sign the requests and then use the list-objects API to list the objects in the bucket.
            yield from self.list_objs_v2(prefix)
            return
        paginator = self.client.get_paginator('list_objects_v2')
        if prefix:
            iterator = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
        else:
            iterator = paginator.paginate(Bucket=self.bucket)
        for page in iterator:
            for content in page.get('Contents', []):
                tokens = content.get('Key', '')
                if tokens:
                    obj = [tokens, content.get('Size', 0)]
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

    def get_signature(self, date):
        sign_str = f"GET\n\n\n{date}\n/{self.bucket}/"

        hmac_object = hmac.new(self.conf.key.encode('utf-8'), sign_str.encode('utf-8'), hashlib.sha1)
        hmac_bytes = hmac_object.digest()
        signature = base64.b64encode(hmac_bytes).decode('utf-8')
        return signature

    def list_bucket_v2 (self, marker, prefix):
        now = datetime.utcnow()
        date = now.strftime('%a, %d %b %Y %H:%M:%S GMT')
        signature =self.get_signature(date)

        headers = {'Date':date,
                'Authorization': f"AWS {self.conf.key_id}:{signature}",
                }

        endpoint_url = self.endpoint_url
        bucket = self.bucket

        if marker and prefix:
            url = f'{endpoint_url}/{bucket}/?marker={marker}&prefix={prefix}/'
        elif marker:
            url = f'{endpoint_url}/{bucket}/?marker={marker}'
        elif prefix: 
            url = f'{endpoint_url}/{bucket}/?prefix={prefix}/'
        else:
            url = f'{endpoint_url}/{bucket}/'

        response = requests.get (url, headers=headers, timeout=300)
        if response.status_code != 200:
            return None
        return response.text

    def list_objs_v2(self, prefix):
        is_truncated = True
        marker = None
        while is_truncated:
            rsp = self.list_bucket_v2(marker, prefix)
            if not rsp:
                break

            root = etree.fromstring(rsp.encode('utf-8'))
            if "ListBucketResult" not in root.tag:
                break
            for child in root:
                if "IsTruncated" in child.tag:
                    if child.text == "true":
                        is_truncated = True
                    else:
                        is_truncated = False
                if "Contents" in child.tag:
                    obj = [] 
                    for contents in child:
                        if "Key" in contents.tag:
                            marker = contents.text
                            obj.append(contents.text)
                        if "Size" in contents.tag:
                            obj.append(int(contents.text))
                    if obj:
                        yield obj
