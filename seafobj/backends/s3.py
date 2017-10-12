from .base import AbstractObjStore

import boto
import boto.s3.connection
from boto.s3.key import Key

class S3Conf(object):
    def __init__(self, key_id, key, bucket_name, host, port, use_v4_sig, aws_region):
        self.key_id = key_id
        self.key = key
        self.bucket_name = bucket_name
        self.host = host
        self.port = port
        self.use_v4_sig = use_v4_sig
        self.aws_region = aws_region

class SeafS3Client(object):
    '''Wraps a s3 connection and a bucket'''
    def __init__(self, conf):
        self.conf = conf
        self.conn = None
        self.bucket = None

    def do_connect(self):
        if self.conf.host is None:
            # If version 4 signature is used, boto requires 'host' parameter
            # Also there is a bug in AWS Frankfurt that causes boto doesn't work.
            # The current work around is to give specific service address, like
            # s3.eu-central-1.amazonaws.com instead of s3.amazonaws.com.
            if self.conf.use_v4_sig:
                self.conn = boto.connect_s3(self.conf.key_id, self.conf.key,
                                            host='s3.%s.amazonaws.com' % self.conf.aws_region)
            else:
                self.conn = boto.connect_s3(self.conf.key_id, self.conf.key)
        else:
            if self.conf.use_v4_sig:
                self.conn = boto.connect_s3(self.conf.key_id, self.conf.key,
                                            host='%s' % self.conf.host)
            else:
                self.conn = boto.connect_s3(
                    aws_access_key_id=self.conf.key_id,
                    aws_secret_access_key=self.conf.key,
                    port=self.conf.port,
                    host=self.conf.host,
                    is_secure=False,
                    calling_format=boto.s3.connection.OrdinaryCallingFormat())

        self.bucket = self.conn.get_bucket(self.conf.bucket_name)

    def read_object_content(self, obj_id):
        if not self.conn:
            self.do_connect()

        k = Key(bucket=self.bucket, name=obj_id)

        return k.get_contents_as_string()

class SeafObjStoreS3(AbstractObjStore):
    '''S3 backend for seafile objecs'''
    def __init__(self, compressed, s3_conf, crypto=None):
        AbstractObjStore.__init__(self, compressed, crypto)
        self.s3_client = SeafS3Client(s3_conf)

    def read_obj_raw(self, repo_id, version, obj_id):
        real_obj_id = '%s/%s' % (repo_id, obj_id)
        data = self.s3_client.read_object_content(real_obj_id)
        return data

    def get_name(self):
        return 'S3 storage backend'

    def list_objs(self):

        if not self.s3_client.conn:
            self.s3_client.do_connect()

        keys = self.s3_client.bucket.list()
        for key in keys:
            tokens = key.name.split('/')
            if len(tokens) == 2:
                repo_id = tokens[0]
                obj_id = tokens[1]
                obj = [repo_id, obj_id]
                yield obj

    def obj_exists(self, repo_id, obj_id):
        if not self.s3_client.conn or not self.s3_client.bucket:
            self.s3_client.do_connect()

        bucket = self.s3_client.bucket
        s3_path = '%s/%s' % (repo_id, obj_id)
        key = Key(bucket=bucket, name=s3_path)
        exists = key.exists()
        self.dest_key = key

        return exists

    def write_obj(self, data, repo_id, obj_id):
        if not self.s3_client.conn or not self.s3_client.bucket:
            self.s3_client.do_connect()

        bucket = self.s3_client.bucket
        s3_path = '%s/%s' % (repo_id, obj_id)
        key = Key(bucket=bucket, name=s3_path)
        key.set_contents_from_string(data)

