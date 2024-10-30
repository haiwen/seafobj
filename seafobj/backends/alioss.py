from .base import AbstractObjStore

from seafobj.exceptions import GetObjectError
from objwrapper.alioss import SeafOSSClient

class SeafObjStoreOSS(AbstractObjStore):
    '''OSS backend for seafile objects'''
    def __init__(self, compressed, oss_conf, crypto=None, cache=None):
        AbstractObjStore.__init__(self, compressed, crypto, cache)
        self.oss_client = SeafOSSClient(oss_conf)
        self.bucket_name = oss_conf.bucket_name

    def read_obj_raw(self, repo_id, version, obj_id):
        real_obj_id = '%s/%s' % (repo_id, obj_id)
        data = self.oss_client.read_obj(real_obj_id)
        return data

    def get_name(self):
        return 'OSS storage backend'

    def list_objs(self, repo_id=None):
        objs = self.oss_client.list_objs(repo_id)
        for obj in objs:
            tokens = obj[0].split('/', 1)
            if len(tokens) == 2:
                newobj = [tokens[0], tokens[1], obj[1]]
                yield newobj

    def obj_exists(self, repo_id, obj_id):
        key = '%s/%s' % (repo_id, obj_id)

        return self.oss_client.obj_exists(key)

    def write_obj(self, data, repo_id, obj_id):
        key = '%s/%s' % (repo_id, obj_id)

        self.oss_client.write_obj(data, key)

    def remove_obj(self, repo_id, obj_id):
        key = '%s/%s' % (repo_id, obj_id)

        self.oss_client.remove_obj(key)
    
    def stat_raw(self, repo_id, obj_id):
        key = '%s/%s' % (repo_id, obj_id)
        
        return self.oss_client.stat_obj(key)

    def get_container_name(self):
        return self.bucket_name
