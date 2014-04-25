import os
import ConfigParser

from seafobj.exceptions import InvalidConfigError
from seafobj.backends.filesystem import SeafObjStoreFS

seafile_conf_dir = os.environ['SEAFILE_CONF_DIR']
seafile_conf = os.path.join(seafile_conf_dir, 'seafile.conf')

def get_s3_conf(cfg, section):
    key_id = cfg.get(section, 'key_id')
    key = cfg.get(section, 'key')
    bucket = cfg.get(section, 'bucket')

    host = None
    port = None
    if cfg.has_option(section, 'host'):
        addr = cfg.get(section, 'host')
        if ':' not in addr:
            raise InvalidConfigError('invalid s3 host %s' % addr)

        segs = addr.split(':')
        if len(segs) != 2:
            raise InvalidConfigError('invalid s3 host %s' % addr)

        host = segs[0]

        try:
            port = int(segs[1])
        except ValueError:
            raise InvalidConfigError('invalid s3 host %s' % addr)

    from seafobj.backends.s3 import S3Conf
    conf = S3Conf(key_id, key, bucket, host, port)

    return conf

class SeafObjStoreFactory(object):
    obj_section_map = {
        'blocks': 'block_backend',
        'fs': 'fs_object_backend',
        'commits': 'commit_object_backend',
    }
    def __init__(self):
        self.seafile_cfg = None

    def get_obj_store(self, obj_type):
        '''Return an implementation of SeafileObjStore'''
        if self.seafile_cfg is None:
            self.seafile_cfg = ConfigParser.ConfigParser()
            try:
                self.seafile_cfg.read(seafile_conf)
            except Exception, e:
                raise InvalidConfigError(str(e))

        try:
            section = self.obj_section_map[obj_type]
        except KeyError:
            raise RuntimeError('unknown obj_type ' + obj_type)

        if self.seafile_cfg.has_option(section, 'name'):
            backend_name = self.seafile_cfg.get(section, 'name')
        else:
            backend_name = 'fs'

        compressed = obj_type == 'fs'
        if backend_name == 'fs':
            obj_dir = os.path.join(seafile_conf_dir, 'storage', obj_type)
            return SeafObjStoreFS(compressed, obj_dir)

        elif backend_name == 's3':
            # We import s3 backend here to avoid depenedency on boto for users
            # not using s3
            from seafobj.backends.s3 import SeafObjStoreS3
            s3_conf = get_s3_conf(self.seafile_cfg, section)
            return SeafObjStoreS3(compressed, s3_conf)

        else:
            raise InvalidConfigError('unknown %s backend "%s"' % (obj_type, backend_name))

objstore_factory = SeafObjStoreFactory()