import http.client
import oss2
from objwrapper.exceptions import InvalidConfigError

# set log level to WARNING
# the api set_file_logger exists after oss2 2.6.0, which has a lot of 'INFO' log
try:
    log_file_path = "log.log"
    oss2.set_file_logger(log_file_path, 'oss2', logging.WARNING)
except:
    pass

class OSSConf(object):
    def __init__(self, key_id, key, bucket_name, host, region, use_https):
        if not host and not region:
            raise InvalidConfigError('endpoint and region are not configured')
        self.host = host
        if not host:
            self.host = 'oss-cn-%s-internal.aliyuncs.com' % region
        self.key_id = key_id
        self.key = key
        self.bucket_name = bucket_name
        self.region = region
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

    def read_obj(self, obj_id):
        res = self.bucket.get_object(obj_id)
        return res.read()

    def get_name(self):
        return 'OSS storage backend'

    def list_objs(self, prefix=None):
        for key in oss2.ObjectIterator(self.bucket, prefix=prefix):
            token = key.key
            if token:
                size = key.size
                obj = [token, size]
                yield obj

    def obj_exists(self, key):
        return self.bucket.object_exists(key)

    def write_obj(self, data, key, ctime=-1):
        headers = None
        if ctime >= 0:
            headers = {'x-oss-meta-ctime':str(ctime)}
        self.bucket.put_object(key, data, headers=headers)

    def remove_obj(self, key):
        self.bucket.delete_object(key)
    
    def stat_obj(self, key):
        size = self.bucket.get_object_meta(key).headers['Content-Length']
        return int(size)

    def get_ctime(self, key):
        headers = self.bucket.head_object(key).headers
        ctime = headers.get('x-oss-meta-ctime', '')
        try:
            return float(ctime)
        except:
            return 0
