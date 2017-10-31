#coding: UTF-8

import zlib

class AbstractObjStore(object):
    '''Base class of seafile object backend'''
    def __init__(self, compressed, crypto=None):
        self.compressed = compressed
        self.crypto = crypto

    def read_obj(self, repo_id, version, obj_id, use_cache=False):
        data = self.read_obj_raw(repo_id, version, obj_id, use_cache)
        if self.crypto:
            data = self.crypto.dec_data(data)
        if self.compressed and version == 1:
            data = zlib.decompress(data)

        return data

    def read_obj_raw(self, repo_id, version, obj_id, use_cache=False):
        '''Read the raw content of the object from the backend. Each backend
        subclass should have their own implementation.

        '''
        raise NotImplementedError

    def get_name(self):
        '''Get the backend name for display in the log'''
        raise NotImplementedError

    def list_objs(self):
        '''List all objects'''
        raise NotImplementedError

    def obj_exists(self, repo_id, obj_id):
        raise NotImplementedError

    def write_obj(self, data, repo_id, obj_id):
        '''Write data to destination backend'''
        raise NotImplementedError

    def objcache_key(self, repo_id, obj_id):
        return repo_id + '-' + obj_id
