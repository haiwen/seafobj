from .base import AbstractObjStore

from seafobj.exceptions import GetObjectError
import httplib
import oss2

# set log level to WARNING
# the api set_file_logger exists after oss2 2.6.0, which has a lot of 'INFO' log
try:
    log_file_path = "log.log"
    oss2.set_file_logger(log_file_path, 'oss2', logging.WARNING)
except:
    pass

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

    def read_obj_raw(self, repo_id, version, obj_id):
        real_obj_id = '%s/%s' % (repo_id, obj_id)
        data = self.oss_client.read_object_content(real_obj_id)
        return data

    def get_name(self):
        return 'OSS storage backend'

    def list_objs(self, repo_id=None):
        object_list = []
        next_marker = ''
        while (1):
            if repo_id != None:
                Simp_obj_info = self.oss_client.bucket.list_objects(repo_id, '',next_marker)
            else:
                Simp_obj_info = self.oss_client.bucket.list_objects('', '', next_marker)

            object_list = Simp_obj_info.object_list

            for key in object_list:
                token = key.key.split('/')
                if len(token) == 2:
                    repo_id = token[0]
                    obj_id = token[1]
                    size = key.size
                    obj = [repo_id, obj_id, size]
                    yield obj

            if Simp_obj_info.is_truncated == False:
                break
            else:
                next_marker = Simp_obj_info.next_marker

    def obj_exists(self, repo_id, obj_id):
        key = '%s/%s' % (repo_id, obj_id)

        return self.oss_client.bucket.object_exists(key)

    def write_obj(self, data, repo_id, obj_id):
        key = '%s/%s' % (repo_id, obj_id)

        self.oss_client.bucket.put_object(key, data)
