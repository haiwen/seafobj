import Queue
import threading

import rados

from .base import AbstractObjStore

from seafobj.utils import to_utf8
from seafobj.utils.ceph_utils import ioctx_set_namespace
from rados import LIBRADOS_ALL_NSPACES

class CephConf(object):
    def __init__(self, ceph_conf_file, pool_name, ceph_client_id=None):
        self.pool_name = pool_name
        self.ceph_conf_file = ceph_conf_file
        self.ceph_client_id = ceph_client_id

class IoCtxPool(object):
    '''since we need to set the namespace before read the object, we need to
    use a different ioctx per thread.

    '''
    def __init__(self, conf, pool_size=5):
        self.conf = conf
        self.pool = Queue.Queue(pool_size)
        if conf.ceph_client_id:
            self.cluster = rados.Rados(conffile=conf.ceph_conf_file, rados_id=conf.ceph_client_id)
        else:
            self.cluster = rados.Rados(conffile=conf.ceph_conf_file)
        self.lock = threading.Lock()

    def get_ioctx(self, repo_id):
        try:
            ioctx = self.pool.get(False)
        except Queue.Empty:
            ioctx = self.create_ioctx()

        ioctx_set_namespace(ioctx, repo_id)

        return ioctx

    def create_ioctx(self):
        with self.lock:
            if self.cluster.state != 'connected':
                self.cluster.connect()

        ioctx = self.cluster.open_ioctx(self.conf.pool_name)

        return ioctx

    def return_ioctx(self, ioctx):
        try:
            self.pool.put(ioctx, False)
        except Queue.Full:
            ioctx.close()

class SeafCephClient(object):
    '''Wraps a Ceph ioctx'''
    def __init__(self, conf):
        self.ioctx_pool = IoCtxPool(conf)

    def read_object_content(self, repo_id, obj_id):
        repo_id = to_utf8(repo_id)
        obj_id = to_utf8(obj_id)

        ioctx = self.ioctx_pool.get_ioctx(repo_id)

        try:
            stat = ioctx.stat(obj_id)
            return ioctx.read(obj_id, length=stat[0])
        finally:
            self.ioctx_pool.return_ioctx(ioctx)

class SeafObjStoreCeph(AbstractObjStore):
    '''Ceph backend for seafile objects'''
    def __init__(self, compressed, ceph_conf, crypto=None):
        AbstractObjStore.__init__(self, compressed, crypto)
        self.ceph_client = SeafCephClient(ceph_conf)

    def read_obj_raw(self, repo_id, version, obj_id):
        data = self.ceph_client.read_object_content(repo_id, obj_id)
        return data

    def get_name(self):
        return 'Ceph storage backend'

    def list_objs(self):
        ioctx = self.ceph_client.ioctx_pool.get_ioctx(LIBRADOS_ALL_NSPACES)
        objs = ioctx.list_objects()
        for obj in objs:
            yield [obj.nspace, obj.key]

        self.ceph_client.ioctx_pool.return_ioctx(ioctx)

    def obj_exists(self, repo_id, obj_id):
        ioctx = self.ceph_client.ioctx_pool.get_ioctx(repo_id)
        try:
            ioctx.stat(obj_id)
        except rados.ObjectNotFound:
            return False
        finally:
            self.ceph_client.ioctx_pool.return_ioctx(ioctx)

        return True

    def write_obj(self, data, repo_id, obj_id):
        try:
            ioctx = self.ceph_client.ioctx_pool.get_ioctx(repo_id)
            ioctx.write_full(obj_id, data)
        except Exception:
            raise
        finally:
            self.ceph_client.ioctx_pool.return_ioctx(ioctx)
