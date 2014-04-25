#coding: UTF-8

import zlib

class AbstractObjStore(object):
    '''Base class of seafile object backend'''
    def __init__(self, compressed):
        self.compressed = compressed

    def read_obj(self, repo_id, version, obj_id):
        data = self.read_obj_raw(repo_id, version, obj_id)
        if self.compressed and version == 1:
            data = zlib.decompress(data)

        return data

    def read_obj_raw(self, repo_id, version, obj_id):
        '''Read the raw content of the object from the backend. Each backend
        subclass should have their own implementation.

        '''
        raise NotImplementedError

    def get_name(self):
        '''Get the backend name for display in the log'''
        raise NotImplementedError
