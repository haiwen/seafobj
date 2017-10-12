import os
import ConfigParser
import binascii

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

        segs = addr.split(':')
        host = segs[0]

        try:
            port = int(segs[1])
        except IndexError:
            pass

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

def get_oss_conf(cfg, section):
    key_id = cfg.get(section, 'key_id')
    key = cfg.get(section, 'key')
    bucket = cfg.get(section, 'bucket')
    endpoint = ''
    if cfg.has_option(section, 'endpoint'):
        endpoint = cfg.get(section, 'endpoint')
    if not endpoint:
        region = cfg.get(section, 'region')
        endpoint = 'oss-cn-%s-internal.aliyuncs.com' % region

    host = endpoint

    from seafobj.backends.alioss import OSSConf
    conf = OSSConf(key_id, key, bucket, host)

    return conf

def get_swift_conf(cfg, section):
    user_name = cfg.get(section, 'user_name')
    password = cfg.get(section, 'password')
    container = cfg.get(section, 'container')
    auth_host = cfg.get(section, 'auth_host')
    if not cfg.has_option(section, 'auth_ver'):
        auth_ver = 'v2.0'
    else:
        auth_ver = cfg.get(section, 'auth_ver')
    tenant = cfg.get(section, 'tenant')
    if cfg.has_option(section, 'use_https'):
        use_https = cfg.getboolean(section, 'use_https')
    else:
        use_https = False
    if cfg.has_option(section, 'region'):
        region = cfg.get(section, 'region')
    else:
        region = None

    from seafobj.backends.swift import SwiftConf
    conf = SwiftConf(user_name, password, container, auth_host, auth_ver, tenant, use_https, region)
    return conf

class SeafileConfig(object):
    def __init__(self):
        self.cfg = None
        self.seafile_conf_dir = os.environ['SEAFILE_CONF_DIR']
        self.central_config_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR',
                                                 None)
        confdir = self.central_config_dir or self.seafile_conf_dir
        self.seafile_conf = os.path.join(confdir, 'seafile.conf')

    def get_config_parser(self):
        if self.cfg is None:
            self.cfg = ConfigParser.ConfigParser()
            try:
                self.cfg.read(self.seafile_conf)
            except Exception, e:
                raise InvalidConfigError(str(e))
        return self.cfg

    def get_seaf_crypto(self):
        if not self.cfg.has_option('store_crypt', 'key_path'):
            return None
        key_path = self.cfg.get('store_crypt', 'key_path')
        if not os.path.exists(key_path):
            raise InvalidConfigError('key file %s doesn\'t exist' % key_path)

        key_config = ConfigParser.ConfigParser()
        key_config.read(key_path)
        if not key_config.has_option('store_crypt', 'enc_key') or not \
           key_config.has_option('store_crypt', 'enc_iv'):
            raise InvalidConfigError('Invalid key file %s: incomplete info' % key_path)

        hex_key = key_config.get('store_crypt', 'enc_key')
        hex_iv = key_config.get('store_crypt', 'enc_iv')
        raw_key = binascii.a2b_hex(hex_key)
        raw_iv = binascii.a2b_hex(hex_iv)

        from seafobj.utils.crypto import SeafCrypto
        return SeafCrypto(raw_key, raw_iv)

    def get_seafile_storage_dir(self):
        ccnet_conf_dir = os.environ.get('CCNET_CONF_DIR', '')
        if ccnet_conf_dir:
            seafile_ini = os.path.join(ccnet_conf_dir, 'seafile.ini')
            if not os.access(seafile_ini, os.F_OK):
                raise RuntimeError('%s does not exist', seafile_ini)

            with open(seafile_ini) as f:
                seafile_data_dir = f.readline().rstrip()
                return os.path.join(seafile_data_dir, 'storage')
        else:
            # In order to pass test, if not set CCNET_CONF_DIR env, use follow path
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

        crypto = self.seafile_cfg.get_seaf_crypto()

        if cfg.has_option(section, 'name'):
            backend_name = cfg.get(section, 'name')
        else:
            backend_name = 'fs'

        compressed = obj_type == 'fs'
        if backend_name == 'fs':
            obj_dir = os.path.join(self.seafile_cfg.get_seafile_storage_dir(), obj_type)
            return SeafObjStoreFS(compressed, obj_dir, crypto)

        elif backend_name == 's3':
            # We import s3 backend here to avoid depenedency on boto for users
            # not using s3
            from seafobj.backends.s3 import SeafObjStoreS3
            s3_conf = get_s3_conf(cfg, section)
            return SeafObjStoreS3(compressed, s3_conf, crypto)

        elif backend_name == 'ceph':
            # We import ceph backend here to avoid depenedency on rados for
            # users not using rados
            from seafobj.backends.ceph import SeafObjStoreCeph
            ceph_conf = get_ceph_conf(cfg, section)
            return SeafObjStoreCeph(compressed, ceph_conf, crypto)

        elif backend_name == 'oss':
            from seafobj.backends.alioss import SeafObjStoreOSS
            oss_conf = get_oss_conf(cfg, section)
            return SeafObjStoreOSS(compressed, oss_conf, crypto)

        elif backend_name == 'swift':
            from seafobj.backends.swift import SeafObjStoreSwift
            swift_conf = get_swift_conf(cfg, section)
            return SeafObjStoreSwift(compressed, swift_conf, crypto)

        else:
            raise InvalidConfigError('unknown %s backend "%s"' % (obj_type, backend_name))

objstore_factory = SeafObjStoreFactory()
