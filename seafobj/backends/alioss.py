from .base import AbstractObjStore

from seafobj.exceptions import GetObjectError
import httplib
import oss2
import pylibmc
import logging

class OSSConf(object):
    def __init__(self, key_id, key, bucket_name, host, cache_host_list=None):
        self.key_id = key_id
        self.key = key
        self.bucket_name = bucket_name
        self.host = host
        self.cache_host_list = cache_host_list

class SeafOSSClient(object):
    '''Wraps a oss connection and a bucket'''
    def __init__(self, conf):
        self.conf = conf
        # Due to a bug in httplib we can't use https
        self.auth = oss2.Auth(conf.key_id, conf.key)
        self.service = oss2.Service(self.auth, conf.host)
        self.bucket = oss2.Bucket(self.auth, conf.host, conf.bucket_name)

    def read_object_content(self, obj_id):
        res = self.bucket.get_object(obj_id)
        return res.read()

class SeafObjStoreOSS(AbstractObjStore):
    '''OSS backend for seafile objects'''
    def __init__(self, compressed, oss_conf, crypto=None):
        AbstractObjStore.__init__(self, compressed, crypto)
        self.oss_client = SeafOSSClient(oss_conf)
        self.conf = oss_conf

        if self.conf.cache_host_list:
            self.get_cache_client()
        else:
            self.cache_client = None

    def get_cache_client(self):
        self.cache_client = pylibmc.Client(self.conf.cache_host_list)
        try:
            self.cache_client.set('test_key', 'test_value')
        except Exception, e:
            logging.warning('Failed to connect memcached: %s', e)
            self.cache_client = None

    def read_obj_raw(self, repo_id, version, obj_id, use_cache):
        data = None
        cache_key = self.objcache_key(repo_id, obj_id)
        if self.cache_client and use_cache:
            try:
                data = self.cache_client.get(cache_key)
            except pylibmc.ConnectionError:
                self.get_cache_client()
                if self.cache_client:
                    data = self.cache_client.get(cache_key)

            if data:
                return data

        real_obj_id = '%s/%s' % (repo_id, obj_id)
        data = self.oss_client.read_object_content(real_obj_id)
        if self.cache_client and use_cache and data:
            self.cache_client.set(cache_key, data)

        return data

    def get_name(self):
        return 'OSS storage backend'

    def list_objs(self):
        Simp_obj_info = self.oss_client.bucket.list_objects()

        for key in Simp_obj_info.object_list:
            token = key.key.split('/')
            if len(token) == 2:
                repo_id = token[0]
                obj_id = token[1]
                obj = [repo_id, obj_id]
                yield obj

    def obj_exists(self, repo_id, obj_id):
        key = '%s/%s' % (repo_id, obj_id)

        return self.oss_client.bucket.object_exists(key)

    def write_obj(self, data, repo_id, obj_id):
        key = '%s/%s' % (repo_id, obj_id)

        self.oss_client.bucket.put_object(key, data)
