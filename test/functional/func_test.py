#coding: UTF-8

import unittest
from nose import SkipTest

import os
import time
import shutil
import tempfile
import subprocess

data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
os.environ['SEAFILE_CONF_DIR'] = data_dir

TEST_REPO_ID = '413c175b-0f7d-4616-8298-22bc147af43c'
TEST_HEAD_COMMIT = '2b216582a86ca7ab72264c3936350363a79c6d23'

from seafobj import commit_mgr, fs_mgr, block_mgr

class FuncTest(unittest.TestCase):
    def setUp(self):
        self.repo_id = TEST_REPO_ID
        self.repo_version = 1
        self.commit_id = TEST_HEAD_COMMIT

    def tearDown(self):
        pass

    def test_read_dir(self):
        commit = commit_mgr.load_commit(self.repo_id, self.repo_version, self.commit_id)
        dir = fs_mgr.load_seafdir(self.repo_id, self.repo_version, commit.root_id)

        import pprint; pprint.pprint(dir.dirents)

        self.assertEquals(len(dir.get_files_list()), 3)
        self.assertEquals(len(dir.get_subdirs_list()), 2)

        dir_a = dir.lookup('folder1')
        self.assertIsNotNone(dir_a)

        dir_b = dir.lookup('第二个中文目录')
        self.assertIsNotNone(dir_b)

        dir_x = dir.lookup('not.exist')
        self.assertIsNone(dir_x)

        file_a = dir.lookup('a.md')
        self.assertIsNotNone(file_a)
        self.assertEquals(file_a.size, 10)
        content = file_a.get_content()
        self.assertEquals(content, 'hello a.md')

        file_b = dir.lookup('一张照片.jpg')
        self.assertIsNotNone(file_b)
        self.assertTrue(file_b.size, 155067)

        # Test read file more than 1 blocks
        file_c = dir.lookup('glib.zip')
        self.assertIsNotNone(file_c)
        self.assertEquals(file_c.size, 3345765)
        content = file_c.get_content()
        with open(os.path.join(data_dir, 'glib.zip'), 'rb') as fp:
            content_r = fp.read()
        self.assertEquals(content, content_r)

        # Test stream read
        stream = file_c.get_stream()
        data = ''
        chunk_size = file_c.size / 5
        for i in xrange(5):
            data += stream.read(chunk_size)
            self.assertEquals(len(data), (i + 1) * chunk_size)
            self.assertEquals(data, content[:len(data)])
        stream.close()

        self.assertEquals(data, content)

        file_x = dir.lookup('not.exist')
        self.assertIsNone(file_x)

    def test_enc_config(self):
        # not set enc_path in conf, crypto object of obj should be None
        self.assertIsNone(commit_mgr.obj_store.crypto)
