#coding: UTF-8

import unittest
from mock import patch, Mock, call
from nose import SkipTest

import os

from seafobj.backends import ceph

class TestIoCtxPool(unittest.TestCase):
    def setUp(self):
        self.mock_cluster_cls = Mock()
        self.mock_cluster = Mock()
        self.mock_cluster_cls.return_value = self.mock_cluster

        self.mock_ioctx_set_namespce = Mock()

        self.conf = ceph.CephConf('test.conf', 'test-pool')
        self.repo_id = 'fake-repo-id'

        with patch.object(ceph.rados, 'Rados', self.mock_cluster_cls):
            self.test_ioctx_pool = ceph.IoCtxPool(self.conf)

    def test_should_create_ioctx_when_pool_empty(self):
        with patch.object(ceph, 'ioctx_set_namespace', self.mock_ioctx_set_namespce):
            io1 = self.test_ioctx_pool.get_ioctx(self.repo_id)

        self.mock_cluster.connect.assert_called_once_with()
        self.mock_cluster.open_ioctx.assert_called_once_with(self.conf.pool_name)
        self.mock_ioctx_set_namespce.assert_called_once_with(io1, self.repo_id)

        self.assertEqual(self.test_ioctx_pool.pool.qsize(), 0)

    def  test_should_not_create_ioctx_if_pool_not_empty(self):
        io = Mock()

        self.test_ioctx_pool.return_ioctx(io)
        self.assertEqual(self.test_ioctx_pool.pool.qsize(), 1)

        io1 = self.test_ioctx_pool.get_ioctx(self.repo_id)
        self.assertEqual(io1, io)
        self.assertEqual(self.test_ioctx_pool.pool.qsize(), 0)

        self.assertEqual(self.mock_cluster.connect.call_count, 0)
        self.assertEqual(self.mock_cluster.open_ioctx.call_count, 0)

class TestSeafObjStoreCeph(unittest.TestCase):
    def test_read_object_should_call_ceph_client(self):
        compressed = False
        conf = ceph.CephConf('test.conf', 'test-pool')
        with patch.object(ceph.rados, 'Rados'):
            test_store = ceph.SeafObjStoreCeph(compressed, conf)

        repo_id = 'fake-repo-id'
        version = 0
        obj_id = 'fake-obj-id'

        with patch.object(test_store, 'ceph_client') as mock_ceph_client:
            test_store.read_obj(repo_id, version, obj_id)

        mock_ceph_client.read_object_content.assert_called_once_with(repo_id, obj_id)

