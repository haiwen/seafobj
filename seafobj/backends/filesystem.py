import os
import zlib

from .base import AbstractObjStore

def id_to_path(dirname, obj_id):
    '''Utility method to format an object path'''
    return os.path.join(dirname, obj_id[:2], obj_id[2:])

class SeafObjStoreFS(AbstractObjStore):
    '''FS backend'''
    def __init__(self, obj_dir, compressed):
        AbstractObjStore.__init__(self)
        self.obj_dir = obj_dir
        self.compressed = compressed

    def read_obj(self, repo_id, version, obj_id):
        compressed = self.compressed and version == 1

        path = id_to_path(os.path.join(self.obj_dir, repo_id), obj_id)

        with open(path, 'rb') as fp:
            data = fp.read()
            
        if compressed:
            data = zlib.decompress(data)
        
        return data
        
    def get_name(self):
        return 'filesystem storage backend'
