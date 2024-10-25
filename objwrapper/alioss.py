import http.client
import oss2

# set log level to WARNING
# the api set_file_logger exists after oss2 2.6.0, which has a lot of 'INFO' log
try:
    log_file_path = "log.log"
    oss2.set_file_logger(log_file_path, 'oss2', logging.WARNING)
except:
    pass

class OSSConf(object):
    def __init__(self, key_id, key, bucket_name, host, use_https):
        self.key_id = key_id
        self.key = key
        self.bucket_name = bucket_name
        self.host = host
        self.use_https = use_https

class SeafOSSClient(object):
    '''Wraps a oss connection and a bucket'''
    def __init__(self, conf):
        self.conf = conf
        if conf.use_https:
            host = 'https://%s' % conf.host
        else:
            host = 'http://%s' % conf.host
        # Due to a bug in httplib we can't use https
        self.auth = oss2.Auth(conf.key_id, conf.key)
        self.service = oss2.Service(self.auth, conf.host)
        self.bucket = oss2.Bucket(self.auth, conf.host, conf.bucket_name)

    def read_object_content(self, obj_id):
        res = self.bucket.get_object(obj_id)
        return res.read()

    def read_obj_raw(self, real_obj_id):
        data = self.read_object_content(real_obj_id)
        return data

    def get_name(self):
        return 'OSS storage backend'

    def list_objs(self, repo_id=None):
        object_list = []
        next_marker = ''
        while True:
            if repo_id:
                Simp_obj_info = self.bucket.list_objects(repo_id, '',next_marker)
            else:
                Simp_obj_info = self.bucket.list_objects('', '', next_marker)

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

    def obj_exists(self, key):
        return self.bucket.object_exists(key)

    def write_obj(self, data, key):
        self.bucket.put_object(key, data)

    def remove_obj(self, key):
        self.bucket.delete_object(key)
    
    def stat_raw(self, key):
        size = self.bucket.get_object_meta(key).headers['Content-Length']
        return int(size)
