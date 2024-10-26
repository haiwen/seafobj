import boto3
from botocore.exceptions import ClientError
from objwrapper.s3 import SeafS3Client

from .base import AbstractObjStore

class SeafObjStoreS3(AbstractObjStore):
    """S3 backend for seafile objects"""
    def __init__(self, compressed, s3_conf, crypto=None, cache=None):
        AbstractObjStore.__init__(self, compressed, crypto, cache)
        self.s3_client = SeafS3Client(s3_conf)

    def read_obj_raw(self, repo_id, version, obj_id):
        real_obj_id = '%s/%s' % (repo_id, obj_id)
        data = self.s3_client.read_obj(real_obj_id)
        return data

    def get_name(self):
        return 'S3 storage backend'

    def list_objs(self, repo_id=None):
        return self.s3_client.list_objs(repo_id)

    def obj_exists(self, repo_id, obj_id):
        s3_path = '%s/%s' % (repo_id, obj_id)
        return self.s3_client.obj_exists(s3_path)

    def write_obj(self, data, repo_id, obj_id):
        s3_path = '%s/%s' % (repo_id, obj_id)
        self.s3_client.write_obj(data, s3_path)

    def remove_obj(self, repo_id, obj_id):
        s3_path = '%s/%s' % (repo_id, obj_id)
        self.s3_client.remove_obj(s3_path)

    def stat_raw(self, repo_id, obj_id):
        s3_path = '%s/%s' % (repo_id, obj_id)
        return self.s3_client.stat_obj(s3_path)
