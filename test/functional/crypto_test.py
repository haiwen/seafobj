#coding: utf-8

import os
import unittest
import json

from seafobj.objstore_factory import SeafObjStoreFactory

data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
#os.environ['SEAFILE_CONF_DIR'] = data_dir
conf_path = os.path.join(data_dir, 'seafile.conf')

TEST_REPO_ID = '413c175b-0f7d-4616-8298-22bc147af43c'
TEST_HEAD_COMMIT = '2b216582a86ca7ab72264c3936350363a79c6d23'

class TestCrypto(unittest.TestCase):
    def setUp(self):
        key_path = os.path.join(data_dir, 'seaf-key.txt')
        with open(conf_path, 'w') as fd:
            fd.write('[store_crypt]\nkey_path = %s' % key_path)

    def tearDown(self):
        with open(conf_path, 'w') as fd:
            fd.truncate(0)

    def test_invalid_data(self):
        obj_store = SeafObjStoreFactory().get_obj_store('commits')
        self.assertIsNotNone(obj_store.crypto)
        self.assertRaises(Exception, obj_store.crypto.enc_data, None)
        self.assertRaises(Exception, obj_store.crypto.dec_data, 'abc')

    def test_enc_dec(self):
        obj_store = SeafObjStoreFactory().get_obj_store('commits')
        self.assertIsNotNone(obj_store.crypto)

        crypto = obj_store.crypto
        obj_store.crypto = None
        # read original data
        dec_data = obj_store.read_obj(TEST_REPO_ID, 1, TEST_HEAD_COMMIT)
        # encrypt original data
        enc_data = crypto.enc_data(dec_data)
        obj_path = os.path.join(obj_store.obj_dir, TEST_REPO_ID, \
                                TEST_HEAD_COMMIT[0:2], TEST_HEAD_COMMIT[2:])
        with open(obj_path, 'wb') as fd:
            fd.truncate(0)
            # write encrypted data to file
            fd.write(enc_data)

        obj_store.crypto = crypto
        # get decrypted data
        dec_data = obj_store.read_obj(TEST_REPO_ID, 1, TEST_HEAD_COMMIT)
        cobj = json.loads(dec_data)
        self.assertEqual(TEST_HEAD_COMMIT, cobj['commit_id'])
        with open(obj_path, 'wb') as fd:
            fd.truncate(0)
            fd.write(dec_data)
