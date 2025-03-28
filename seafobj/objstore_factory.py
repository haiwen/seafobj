import os
import configparser
import binascii
import logging
import json

from sqlalchemy import select

from seafobj.exceptions import InvalidConfigError
from seafobj.backends.filesystem import SeafObjStoreFS
from seafobj.mc import get_mc_cache
from seafobj.redis_cache import get_redis_cache

def get_ceph_conf(cfg, section):
    config_file = cfg.get(section, 'ceph_config')
    pool_name = cfg.get(section, 'pool')
    ceph_client_id = ''
    if cfg.has_option(section, 'ceph_client_id'):
        ceph_client_id = cfg.get(section, 'ceph_client_id')

    from seafobj.backends.ceph import CephConf

    return CephConf(config_file, pool_name, ceph_client_id)

def get_ceph_conf_from_json(cfg):
    config_file = cfg['ceph_config']
    pool_name = cfg['pool']
    ceph_client_id = ''

    if 'ceph_client_id' in cfg:
        ceph_client_id = cfg['ceph_client_id']

    from seafobj.backends.ceph import CephConf
    conf = CephConf(config_file, pool_name, ceph_client_id)

    return conf

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
    if cfg.has_option(section, 'aws_region'):
        aws_region = cfg.get(section, 'aws_region')

    use_https = False
    if cfg.has_option(section, 'use_https'):
        use_https = cfg.getboolean(section, 'use_https')

    path_style_request = False
    if cfg.has_option(section, 'path_style_request'):
        path_style_request = cfg.getboolean(section, 'path_style_request')

    sse_c_key = None
    if cfg.has_option(section, 'sse_c_key'):
        sse_c_key = cfg.get(section, 'sse_c_key')


    from objwrapper.s3 import S3Conf
    conf = S3Conf(key_id, key, bucket, host, port, use_v4_sig, aws_region, use_https, path_style_request, sse_c_key)

    return conf

def get_s3_conf_from_json(cfg):
    key_id = cfg['key_id']
    key = cfg['key']
    bucket = cfg['bucket']

    host = None
    port = None

    if 'host' in cfg:
        addr = cfg['host']

        segs = addr.split(':')
        host = segs[0]

        try:
            port = int(segs[1])
        except IndexError:
            pass
    use_v4_sig = False
    if 'use_v4_signature' in cfg:
        use_v4_sig = cfg['use_v4_signature']

    aws_region = None
    if 'aws_region' in cfg:
        aws_region = cfg['aws_region']

    use_https = False
    if 'use_https' in cfg:
        if str(cfg['use_https']).lower().strip() == 'true':
            use_https = True

    path_style_request = False
    if 'path_style_request' in cfg:
        path_style_request = cfg['path_style_request']

    sse_c_key = None
    if 'sse_c_key' in cfg:
        sse_c_key = cfg['sse_c_key']

    from objwrapper.s3 import S3Conf
    conf = S3Conf(key_id, key, bucket, host, port, use_v4_sig, aws_region, use_https, path_style_request, sse_c_key)

    return conf

def get_oss_conf(cfg, section):
    key_id = cfg.get(section, 'key_id')
    key = cfg.get(section, 'key')
    bucket = cfg.get(section, 'bucket')
    endpoint = ''
    if cfg.has_option(section, 'endpoint'):
        endpoint = cfg.get(section, 'endpoint')
    region = ''
    if cfg.has_option(section, 'region'):
        region = cfg.get(section, 'region')

    use_https = False
    if cfg.has_option(section, 'use_https'):
        use_https = cfg.getboolean(section, 'use_https')

    from objwrapper.alioss import OSSConf
    conf = OSSConf(key_id, key, bucket, endpoint, region, use_https)

    return conf

def get_oss_conf_from_json(cfg):
    key_id = cfg['key_id']
    key = cfg['key']
    bucket = cfg['bucket']

    endpoint = ''

    if 'endpoint' in cfg:
        endpoint = cfg['endpoint']
    region = ''
    if 'region' in cfg:
        region = cfg['region']

    use_https = False
    if 'use_https' in cfg:
        if str(cfg['use_https']).lower().strip() == 'true':
            use_https = True

    from objwrapper.alioss import OSSConf
    conf = OSSConf(key_id, key, bucket, endpoint, region, use_https)

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
    if auth_ver != 'v1.0':
        tenant = cfg.get(section, 'tenant')
    else:
        tenant = None
    if cfg.has_option(section, 'use_https'):
        use_https = cfg.getboolean(section, 'use_https')
    else:
        use_https = False
    if cfg.has_option(section, 'region'):
        region = cfg.get(section, 'region')
    else:
        region = None
    if cfg.has_option(section, 'domain'):
        domain = cfg.get(section, 'domain')
    else:
        domain = 'default'

    from seafobj.backends.swift import SwiftConf
    conf = SwiftConf(user_name, password, container, auth_host, auth_ver, tenant, use_https, region, domain)
    return conf

def get_swift_conf_from_json (cfg):
    user_name = cfg['user_name']
    password = cfg['password']
    container = cfg['container']
    auth_host = cfg['auth_host']
    if 'auth_ver' not in cfg:
        auth_ver = 'v2.0'
    else:
        auth_ver = cfg['auth_ver']
    if auth_ver != 'v1.0':
        tenant = cfg['tenant']
    else:
        tenant = None
    if 'use_https' in cfg and cfg['use_https']:
        use_https = True
    else:
        use_https = False
    if 'region' in cfg:
        region = cfg['region']
    else:
        region = None
    if 'domain' in cfg:
        domain = cfg['domain']
    else:
        domain = 'default'

    from seafobj.backends.swift import SwiftConf
    conf = SwiftConf(user_name, password, container, auth_host, auth_ver, tenant, use_https, region, domain)
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
            self.cfg = configparser.ConfigParser()
            try:
                self.cfg.read(self.seafile_conf)
            except Exception as e:
                raise InvalidConfigError(str(e))
        return self.cfg

    def get_seaf_crypto(self):
        if not self.cfg.has_option('store_crypt', 'key_path'):
            return None
        key_path = self.cfg.get('store_crypt', 'key_path')
        if not os.path.exists(key_path):
            raise InvalidConfigError('key file %s doesn\'t exist' % key_path)

        key_config = configparser.ConfigParser()
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
        if self.seafile_conf_dir and os.path.exists(self.seafile_conf_dir):
            return os.path.join(self.seafile_conf_dir, 'storage')
        raise RuntimeError('environment SEAFILE_CONF_DIR not set correctly.');

    def get_seaf_cache(self):
        envs = os.environ
        cache_provider = envs.get("CACHE_PROVIDER")
        if cache_provider == "redis" or cache_provider == "memcached":
            return self.load_cache_from_env (envs)

        if self.cfg.has_option('redis', 'redis_host'):
            host = self.cfg.get('redis', 'redis_host')
            if self.cfg.has_option('redis', 'redis_port'):
                port = self.cfg.get('redis', 'redis_port')
            else:
                port = 6379
            if self.cfg.has_option('redis', 'redis_expiry'):
                expiry = self.cfg.get('redis', 'redis_expiry')
            else:
                expiry = 24 * 3600
            if self.cfg.has_option('redis', 'max_connections'):
                max_connections = self.cfg.get('redis', 'max_connections')
            else:
                max_connections = 20
            if self.cfg.has_option('redis', 'redis_password'):
                passwd = self.cfg.get('redis', 'redis_password')
            else:
                passwd = None
            return get_redis_cache(host, port, expiry, int(max_connections), passwd)

        if self.cfg.has_option('memcached', 'memcached_options'):
            mc_options = self.cfg.get('memcached', 'memcached_options')
            if self.cfg.has_option('memcached', 'memcached_expiry'):
                expiry = self.cfg.get('memcached', 'memcached_expiry')
            else:
                expiry = 24 * 3600

            return get_mc_cache(mc_options, int(expiry))
        return None

    def load_cache_from_env(self, envs):
        cache_provider = envs.get("CACHE_PROVIDER")
        if cache_provider == "redis":
            host = envs.get("REDIS_HOST")
            if not host:
                return None
            if envs.get("REDIS_PORT"):
                port = int(envs.get("REDIS_PORT"))
            else:
                port = 6379
            if envs.get("REDIS_MAX_CONNECTIONS"):
                max_connections = int(envs.get("REDIS_MAX_CONNECTIONS"))
            else:
                max_connections = 100
            if envs.get("REDIS_EXPIRY"):
                expiry = int(envs.get("REDIS_EXPIRY"))
            else:
                expiry = 24 * 3600
            passwd = envs.get("REDIS_PASSWORD")
            return get_redis_cache(host, port, expiry, max_connections, passwd)
        elif cache_provider == "memcached":
            host = envs.get("MEMCACHED_HOST")
            if not host:
                return None
            if envs.get("MEMCACHED_PORT"):
                port = int(envs.get("MEMCACHED_PORT"))
            else:
                port = 11211
            mc_options = f"--SERVER={host}:{port} --POOL-MIN=10 --POOL-MAX=100" 
            if envs.get("MEMCACHED_EXPIRY"):
                expiry = int(envs.get("MEMCACHED_EXPIRY"))
            else:
                expiry = 24 * 3600
            return get_mc_cache(mc_options, expiry)


# You must ensure that the SeafObjStoreFactory is created in the main thread.
# If you're using a high-level wrapper like SeafCommitManager or SeafFSManager, it will automatically be created in the main thread.

class SeafObjStoreFactory(object):
    obj_section_map = {
        'blocks': 'block_backend',
        'fs': 'fs_object_backend',
        'commits': 'commit_object_backend',
    }
    def __init__(self, cfg=None):
        self.seafile_cfg = cfg or SeafileConfig()
        self.json_cfg = None
        self.enable_storage_classes = False
        self.obj_stores = {'commits': {}, 'fs': {}, 'blocks': {}}

        cfg = self.seafile_cfg.get_config_parser()
        if cfg.has_option ('storage', 'enable_storage_classes'):
            enable_storage_classes = cfg.get('storage', 'enable_storage_classes')
            if enable_storage_classes.lower() == 'true':
                from seafobj.db import init_db_session_class
                self.enable_storage_classes = True
                self.session = init_db_session_class(cfg)
                try:
                    json_file = cfg.get('storage', 'storage_classes_file')
                    f = open(json_file)
                    self.json_cfg = json.load(f)
                except Exception:
                    logging.warning('Failed to load json file')
                    raise

    def get_obj_stores(self, obj_type):
        try:
            if self.obj_stores[obj_type]:
                return self.obj_stores[obj_type]
        except KeyError:
            raise RuntimeError('unknown obj_type ' + obj_type)

        for bend in self.json_cfg:
            storage_id = bend['storage_id']

            crypto = self.seafile_cfg.get_seaf_crypto()
            compressed = obj_type == 'fs'
            cache = None
            if obj_type != 'blocks':
                cache = self.seafile_cfg.get_seaf_cache()

            if bend[obj_type]['backend'] == 'fs':
                obj_dir = os.path.join(bend[obj_type]['dir'], 'storage', obj_type)
                self.obj_stores[obj_type][storage_id] = SeafObjStoreFS(compressed, obj_dir, crypto)
            elif bend[obj_type]['backend'] == 'swift':
                from seafobj.backends.swift import SeafObjStoreSwift
                swift_conf = get_swift_conf_from_json(bend[obj_type])
                self.obj_stores[obj_type][storage_id] = SeafObjStoreSwift(compressed, swift_conf, crypto, cache)
            elif bend[obj_type]['backend'] == 's3':
                from seafobj.backends.s3 import SeafObjStoreS3
                s3_conf = get_s3_conf_from_json(bend[obj_type])
                self.obj_stores[obj_type][storage_id] = SeafObjStoreS3(compressed, s3_conf, crypto, cache)
            elif bend[obj_type]['backend'] == 'ceph':
                from seafobj.backends.ceph import SeafObjStoreCeph
                ceph_conf = get_ceph_conf_from_json(bend[obj_type])
                self.obj_stores[obj_type][storage_id] = SeafObjStoreCeph(compressed, ceph_conf, crypto, cache)
            elif bend[obj_type]['backend'] == 'oss':
                from seafobj.backends.alioss import SeafObjStoreOSS
                oss_conf = get_oss_conf_from_json(bend[obj_type])
                self.obj_stores[obj_type][storage_id] = SeafObjStoreOSS(compressed, oss_conf, crypto, cache)
            else:
                raise InvalidConfigError('Unknown backend type: %s.' % bend[obj_type]['backend'])

            if 'is_default' in bend and bend['is_default']==True:
                if '__default__' in self.obj_stores[obj_type]:
                    raise InvalidConfigError('Only one default backend can be set.')
                self.obj_stores[obj_type]['__default__'] = self.obj_stores[obj_type][storage_id]

        return self.obj_stores[obj_type]

    def get_obj_store(self, obj_type):
        '''Return an implementation of SeafileObjStore'''
        cfg = self.seafile_cfg.get_config_parser()
        try:
            section = self.obj_section_map[obj_type]
        except KeyError:
            raise RuntimeError('unknown obj_type ' + obj_type)

        crypto = self.seafile_cfg.get_seaf_crypto()
        cache = None
        if obj_type != 'blocks':
            cache = self.seafile_cfg.get_seaf_cache()

        if cfg.has_option(section, 'name'):
            backend_name = cfg.get(section, 'name')
        else:
            backend_name = 'fs'
        
        dir_path = None
        if cfg.has_option(section, 'dir'):
            dir_path = cfg.get(section, 'dir')

        compressed = obj_type == 'fs'
        if backend_name == 'fs':
            if dir_path is None:
                obj_dir = os.path.join(self.seafile_cfg.get_seafile_storage_dir(), obj_type)
            else:
                obj_dir = os.path.join(dir_path, 'storage', obj_type)
            if not os.path.exists(obj_dir):
                os.makedirs(obj_dir)
            return SeafObjStoreFS(compressed, obj_dir, crypto)

        elif backend_name == 's3':
            # We import s3 backend here to avoid dependency on boto3 for users
            # not using s3
            from seafobj.backends.s3 import SeafObjStoreS3
            s3_conf = get_s3_conf(cfg, section)
            return SeafObjStoreS3(compressed, s3_conf, crypto, cache)

        elif backend_name == 'ceph':
            # We import ceph backend here to avoid depenedency on rados for
            # users not using rados
            from seafobj.backends.ceph import SeafObjStoreCeph
            ceph_conf = get_ceph_conf(cfg, section)
            return SeafObjStoreCeph(compressed, ceph_conf, crypto, cache)

        elif backend_name == 'oss':
            from seafobj.backends.alioss import SeafObjStoreOSS
            oss_conf = get_oss_conf(cfg, section)
            return SeafObjStoreOSS(compressed, oss_conf, crypto, cache)

        elif backend_name == 'swift':
            from seafobj.backends.swift import SeafObjStoreSwift
            swift_conf = get_swift_conf(cfg, section)
            return SeafObjStoreSwift(compressed, swift_conf, crypto, cache)

        else:
            raise InvalidConfigError('unknown %s backend "%s"' % (obj_type, backend_name))

objstore_factory = SeafObjStoreFactory()
repo_storage_id = {}

def get_repo_storage_id(repo_id):
    if repo_id in repo_storage_id:
        return repo_storage_id[repo_id]
    else:
        from .db import Base
        from sqlalchemy.orm.scoping import scoped_session
        RepoStorageId = Base.classes.RepoStorageId
        storage_id = None
        session = scoped_session(objstore_factory.session)
        r = session.scalars(select(RepoStorageId).where(RepoStorageId.repo_id == repo_id).limit(1)).first()
        storage_id = r.storage_id if r else None
        repo_storage_id[repo_id] = storage_id
        session.remove()
        return storage_id
