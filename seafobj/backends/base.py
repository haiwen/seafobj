#coding: UTF-8

import zlib
from seafobj.exceptions import GetObjectError

class AbstractObjStore(object):
    '''Base class of seafile object backend'''
    def __init__(self, compressed, crypto=None):
        self.compressed = compressed
        self.crypto = crypto

    def read_obj(self, repo_id, version, obj_id):
        try:
            data = self.read_obj_raw(repo_id, version, obj_id)
            if self.crypto:
                data = self.crypto.dec_data(data)
            if self.compressed and version == 1:
                data = zlib.decompress(data)
        except Exception as e:
            raise GetObjectError('Failed to read object %s/%s: %s' % (repo_id, obj_id, e))

        return data

    def read_decrypted(self, repo_id, version, obj_id):
        try:
            data = self.read_obj_raw(repo_id, version, obj_id)
            if self.crypto:
                data = self.crypto.dec_data(data)
        except Exception as e:
            raise GetObjectError('Failed to read decrypted object %s/%s: %s' % (repo_id, obj_id, e))

        return data

    def read_obj_raw(self, repo_id, version, obj_id):
        '''Read the raw content of the object from the backend. Each backend
        subclass should have their own implementation.

        '''
        raise NotImplementedError

    def get_name(self):
        '''Get the backend name for display in the log'''
        raise NotImplementedError

    def list_objs(self, repo_id=None):
        '''List all objects'''
        raise NotImplementedError

    def obj_exists(self, repo_id, obj_id):
        raise NotImplementedError

    def write_obj(self, data, repo_id, obj_id):
        '''Write data to destination backend'''
        raise NotImplementedError
    
    def stat(self, repo_id, verison, obj_id):
        if self.crypto or self.compressed:
            try:
                data = self.read_obj(repo_id, verison, obj_id)
                return len(data)
            except:
                raise
        return self.stat_raw(repo_id, obj_id)
        
    def stat_raw(self, repo_id, obj_id):
        raise NotImplementedError

    def get_container_name(self):
        raise NotImplementedError
