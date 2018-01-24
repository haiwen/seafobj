import os
import time
import unittest

from seafobj.fs import SeafDirent, SeafDir, SeafFile

class BaseTest(unittest.TestCase):
    top_dir = os.path.join(os.path.dirname(__file__), 'functional', 'data', 'storage')
    dirent_time = time.time()
    dirent_size = 1024
    dirent_id = 'test_id'
    dir_obj_id = '0' * 40
    dir_store_id = '1' * 40
    file_obj_id = '3' * 40
    file_store_id = '4' * 40
    file_size = 2048
    @classmethod
    def get_file_dirent_v1(cls, *args, **kwargs):
        return SeafDirent.fromV1('v1_file', SeafDirent.FILE, cls.dirent_id, cls.dirent_time, cls.dirent_size)

    @classmethod
    def get_dir_dirent_v1(cls, *args, **kwargs):
        return SeafDirent.fromV1('v1_dir', SeafDirent.DIR, cls.dirent_id, cls.dirent_time, cls.dirent_size)

    @classmethod
    def get_dir_dirent_v0(cls, *args, **kwargs):
        return SeafDirent.fromV0('v0_dir', SeafDirent.DIR, cls.dirent_id)

    @classmethod
    def get_file_dirent_v0(cls, *args, **kwargs):
        return SeafDirent.fromV0('v0_file', SeafDirent.FILE, cls.dirent_id)

    @classmethod
    def get_seaf_dir_v0(cls, *args, **kwargs):
        return SeafDir(cls.dir_store_id, 0, cls.dir_obj_id, {})

    @classmethod
    def get_seaf_dir_v1(cls, *args, **kwargs):
        return SeafDir(cls.dir_store_id, 1, cls.dir_obj_id, {})

    @classmethod
    def get_seaffile_v0(cls, *args, **kwargs):
        return SeafFile(cls.file_store_id, 0, cls.file_obj_id, [], cls.file_size)

    @classmethod
    def get_seaffile_v1(cls, *args, **kwargs):
        return SeafFile(cls.file_store_id, 1, cls.file_obj_id, [], cls.file_size)

    @classmethod
    def write_obj(cls, data, obj, repo_id, obj_id, *args, **kwargs):
        tmp_path = os.path.join(cls.top_dir, obj, repo_id, obj_id[:2], obj_id[2:])
        if not os.path.exists(os.path.dirname(tmp_path)):
            os.makedirs(os.path.dirname(tmp_path))
        with open(tmp_path, 'w') as fd:
            fd.write(data)
