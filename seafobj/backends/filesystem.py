import os
import errno
from tempfile import NamedTemporaryFile

from .base import AbstractObjStore

def id_to_path(dirname, obj_id):
    '''Utility method to format an object path'''
    return os.path.join(dirname, obj_id[:2], obj_id[2:])

class SeafObjStoreFS(AbstractObjStore):
    '''FS backend'''
    def __init__(self, compressed, obj_dir, crypto=None):
        AbstractObjStore.__init__(self, compressed, crypto)
        self.obj_dir = obj_dir
        self.compressed = compressed

    def read_obj_raw(self, repo_id, version, obj_id):
        path = id_to_path(os.path.join(self.obj_dir, repo_id), obj_id)

        with open(path, 'rb') as fp:
            data = fp.read()
            
        return data
        
    def get_name(self):
        return 'filesystem storage backend'

    def list_objs(self, repo_id=None):
        top_path = self.obj_dir
        if repo_id:
            repo_path = os.path.join(top_path, repo_id)
            if not os.path.exists(repo_path):
                return
            for spath in os.listdir(repo_path):
                obj_path = os.path.join(repo_path, spath)
                if not os.path.exists(obj_path):
                    continue
                for lpath in os.listdir(obj_path):
                    obj_id = spath + lpath
                    obj = [repo_id, obj_id, 0]
                    yield obj
        else:
            if not os.path.exists(top_path):
                return
            for _repo_id in os.listdir(top_path):
                repo_path = os.path.join(top_path, _repo_id)
                if not os.path.exists(repo_path):
                    return
                for spath in os.listdir(repo_path):
                    obj_path = os.path.join(repo_path, spath)
                    if not os.path.exists(obj_path):
                        continue
                    for lpath in os.listdir(obj_path):
                        obj_id = spath + lpath
                        obj = [_repo_id, obj_id, 0]
                        yield obj

    def obj_exists(self, repo_id, obj_id):
        dirname = self.obj_dir
        filepath = os.path.join(dirname, repo_id, obj_id[:2], obj_id[2:])

        return os.path.exists(filepath)

    def write_obj(self, data, repo_id, obj_id):
        path = os.path.join(self.obj_dir, repo_id, obj_id[:2])
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

        with NamedTemporaryFile(mode='w+b', delete=False) as fp:
            fp.write(data)

        filename = os.path.join(path, obj_id[2:])
        os.rename(fp.name, filename)
    
    def remove_obj(self, repo_id, obj_id):
        path = os.path.join(self.obj_dir, repo_id, obj_id[:2], obj_id[2:])

        if os.path.exists(path):
            try:
                os.remove(path)
            except OSError as e:
                raise
    
    def stat_raw(self, repo_id, obj_id):
        path = os.path.join(self.obj_dir, repo_id, obj_id[:2], obj_id[2:])

        stat_info = None
        if os.path.exists(path):
            try:
                stat_info = os.stat(path)
            except OSError as e:
                raise
        if not stat_info:
            return -1
        return stat_info.st_size
