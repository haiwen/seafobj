from .base import AbstractObjStore

from oss.oss_api import OssAPI
from seafobj.exceptions import GetObjectError
import httplib

class OSSConf(object):
    def __init__(self, key_id, key, bucket_name, host):
        self.key_id = key_id
        self.key = key
        self.bucket_name = bucket_name
        self.host = host

class SeafOSSClient(object):
    '''Wraps a oss connection and a bucket'''
    def __init__(self, conf):
        self.conf = conf
        # Due to a bug in httplib we can't use https
        self.oss = OssAPI(conf.host, conf.key_id, conf.key)

    def read_object_content(self, obj_id):
        res = self.oss.get_object(self.conf.bucket_name, obj_id)
        if res.status != httplib.OK:
            raise GetObjectError("Failed to get object %s from bucket %s: %s %s" % (
                                 obj_id, self.conf.bucket_name, res.status, res.reason))
        return res.read()

class SeafObjStoreOSS(AbstractObjStore):
    '''OSS backend for seafile objecs'''
    def __init__(self, compressed, oss_conf, crypto=None):
        AbstractObjStore.__init__(self, compressed, crypto)
        self.oss_client = SeafOSSClient(oss_conf)

    def read_obj_raw(self, repo_id, version, obj_id):
        real_obj_id = '%s/%s' % (repo_id, obj_id)
        data = self.oss_client.read_object_content(real_obj_id)
        return data

    def get_name(self):
        return 'OSS storage backend'
