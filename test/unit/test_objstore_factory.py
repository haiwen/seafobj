#coding: UTF-8

import unittest
from mock import patch, Mock
from nose import SkipTest

import os
import Queue
import urlparse
import StringIO
import ConfigParser

os.environ['SEAFILE_CONF_DIR'] = ''

from seafobj.objstore_factory import SeafObjStoreFactory

from seafobj.backends.filesystem import SeafObjStoreFS
from seafobj.backends.s3 import SeafObjStoreS3

FS_CONF = '''
'''

S3_CONF = '''
[block_backend]
name = s3
bucket = seafile-blocks
key_id = swifttest:testuser
key = testing
host = 192.168.1.124:8080
path_style_request = true
memcached_options = --SERVER=localhost --POOL-MIN=10 --POOL-MAX=100

[commit_object_backend]
name = s3
bucket = seafile-commits
key_id = swifttest:testuser
key = testing
host = 192.168.1.124:8080
path_style_request = true
memcached_options = --SERVER=localhost --POOL-MIN=10 --POOL-MAX=100

[fs_object_backend]
name = s3
bucket = seafile-fs
key_id = swifttest:testuser
key = testing
host = 192.168.1.124:8080
path_style_request = true
memcached_options = --SERVER=localhost --POOL-MIN=10 --POOL-MAX=100
'''

class FakeSeafileConfig(object):
    def __init__(self, content):
        self.cfg = ConfigParser.ConfigParser()

        buf = StringIO.StringIO(content)
        self.cfg.readfp(buf)

    def get_config_parser(self):
        return self.cfg

    def get_seafile_storage_dir(self):
        return ''


# @patch.object(tasks_manager, 'config', fake_config)
class TestObjstoreFactory(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def create_factory_with_conf(self, content):
        return SeafObjStoreFactory(FakeSeafileConfig(content))

    def assert_factory_creates_instance(self, factory, cls):
        self.assertIsInstance(factory.get_obj_store('fs'), cls)
        self.assertIsInstance(factory.get_obj_store('commits'), cls)
        self.assertIsInstance(factory.get_obj_store('blocks'), cls)

    def test_factory_with_fs_backend(self):
        test_factory = self.create_factory_with_conf(FS_CONF)
        self.assert_factory_creates_instance(test_factory, SeafObjStoreFS)

    def verify_s3_conf(self, s3_conf, bucket):
        self.assertEqual(s3_conf.key_id, 'swifttest:testuser')
        self.assertEqual(s3_conf.key, 'testing')
        self.assertEqual(s3_conf.bucket_name, bucket)
        self.assertEqual(s3_conf.host, '192.168.1.124')
        self.assertEqual(s3_conf.port, 8080)

    def test_factory_with_s3_backend(self):
        test_factory = self.create_factory_with_conf(S3_CONF)
        self.assert_factory_creates_instance(test_factory, SeafObjStoreS3)

        store = test_factory.get_obj_store('fs')
        self.verify_s3_conf(store.s3_client.conf, 'seafile-fs')

        store = test_factory.get_obj_store('blocks')
        self.verify_s3_conf(store.s3_client.conf, 'seafile-blocks')

        store = test_factory.get_obj_store('commits')
        self.verify_s3_conf(store.s3_client.conf, 'seafile-commits')
