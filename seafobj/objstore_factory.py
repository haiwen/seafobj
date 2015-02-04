import os
import ConfigParser

from seafobj.exceptions import InvalidConfigError
from seafobj.backends.filesystem import SeafObjStoreFS

def get_ceph_conf(cfg, section):
    config_file = cfg.get(section, 'ceph_config')
    pool_name = cfg.get(section, 'pool')
    ceph_client_id = ''
    if cfg.has_option(section, 'ceph_client_id'):
        ceph_client_id = cfg.get(section, 'ceph_client_id')

    from seafobj.backends.ceph import CephConf

    return CephConf(config_file, pool_name, ceph_client_id)

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

    use_v4_sig = False
    if cfg.has_option(section, 'use_v4_signature'):
        use_v4_sig = cfg.getboolean(section, 'use_v4_signature')

    aws_region = None
    if use_v4_sig:
        if not cfg.has_option(section, 'aws_region'):
            raise InvalidConfigError('aws_region is not configured')
        aws_region = cfg.get(section, 'aws_region')

    from seafobj.backends.s3 import S3Conf
    conf = S3Conf(key_id, key, bucket, host, port, use_v4_sig, aws_region)

    return conf

class SeafileConfig(object):
    def __init__(self):
        self.cfg = None
        self.seafile_conf_dir = os.environ['SEAFILE_CONF_DIR']
        self.seafile_conf = os.path.join(self.seafile_conf_dir, 'seafile.conf')

    def get_config_parser(self):
        if self.cfg is None:
            self.cfg = ConfigParser.ConfigParser()
            try:
                self.cfg.read(self.seafile_conf)
            except Exception, e:
                raise InvalidConfigError(str(e))
        return self.cfg

    def get_seafile_storage_dir(self):
        return os.path.join(self.seafile_conf_dir, 'storage')

class SeafObjStoreFactory(object):
    obj_section_map = {
        'blocks': 'block_backend',
        'fs': 'fs_object_backend',
        'commits': 'commit_object_backend',
    }
    def __init__(self, cfg=None):
        self.seafile_cfg = cfg or SeafileConfig()

    def get_obj_store(self, obj_type):
        '''Return an implementation of SeafileObjStore'''
        cfg = self.seafile_cfg.get_config_parser()
        try:
            section = self.obj_section_map[obj_type]
        except KeyError:
            raise RuntimeError('unknown obj_type ' + obj_type)

        if cfg.has_option(section, 'name'):
            backend_name = cfg.get(section, 'name')
        else:
            backend_name = 'fs'

        compressed = obj_type == 'fs'
        if backend_name == 'fs':
            obj_dir = os.path.join(self.seafile_cfg.get_seafile_storage_dir(), obj_type)
            return SeafObjStoreFS(compressed, obj_dir)

        elif backend_name == 's3':
            # We import s3 backend here to avoid depenedency on boto for users
            # not using s3
            from seafobj.backends.s3 import SeafObjStoreS3
            s3_conf = get_s3_conf(cfg, section)
            return SeafObjStoreS3(compressed, s3_conf)

        elif backend_name == 'ceph':
            # We import ceph backend here to avoid depenedency on rados for
            # users not using rados
            from seafobj.backends.ceph import SeafObjStoreCeph
            ceph_conf = get_ceph_conf(cfg, section)
            return SeafObjStoreCeph(compressed, ceph_conf)

        else:
            raise InvalidConfigError('unknown %s backend "%s"' % (obj_type, backend_name))

objstore_factory = SeafObjStoreFactory()
