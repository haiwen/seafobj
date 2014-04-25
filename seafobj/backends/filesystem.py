import os

from .base import AbstractObjStore

def id_to_path(dirname, obj_id):
    '''Utility method to format an object path'''
    return os.path.join(dirname, obj_id[:2], obj_id[2:])

class SeafObjStoreFS(AbstractObjStore):
    '''FS backend'''
    def __init__(self, compressed, obj_dir):
        AbstractObjStore.__init__(self, compressed)
        self.obj_dir = obj_dir
        self.compressed = compressed

    def read_obj_raw(self, repo_id, version, obj_id):
        path = id_to_path(os.path.join(self.obj_dir, repo_id), obj_id)

        with open(path, 'rb') as fp:
            data = fp.read()
            
        return data
        
    def get_name(self):
        return 'filesystem storage backend'
