import json
import mock
import struct
import threading

from ..base import BaseTest
from seafobj import fs_mgr
from seafobj.fs import SeafFile, SeafileStream

class TestSeafDirent(BaseTest):
    def setUp(self):
        self.file_dirent = self.get_file_dirent_v0()
        self.dir_dirent = self.get_dir_dirent_v0()

    def tearDown(self):
        pass

    def test_file_and_dir(self):
        self.assertTrue(self.dir_dirent.is_dir())
        self.assertFalse(self.dir_dirent.is_file())
        self.assertTrue(self.file_dirent.is_file())
        self.assertFalse(self.file_dirent.is_dir())

    def test_str(self):
        self.assertEqual(str(self.dir_dirent), 'SeafDirent(type=%s, name=%s, size=%s, id=%s, mtime=%s)' % ('dir' if self.dir_dirent.is_dir() else 'file', 'v0_dir', -1, self.dirent_id, -1))
        self.assertEqual(str(self.file_dirent), 'SeafDirent(type=%s, name=%s, size=%s, id=%s, mtime=%s)' % ('dir' if self.file_dirent.is_dir() else 'file', 'v0_file', -1, self.dirent_id, -1))

def mock_return_args(*args):
    return args


class TestSeafDir(BaseTest):
    def setUp(self):
        self.storage_id = '0' * 40
        self.obj_id = '1' * 40
        self.file_dirent = self.get_file_dirent_v0()
        self.dir_dirent = self.get_dir_dirent_v0()
        self.testmd = self.get_file_dirent_v1()
        self.dirents = {'folder': self.dir_dirent, 'test.md': self.testmd, 'file': self.file_dirent}
        self.dir = self.get_seaf_dir_v0()
        self.dir.dirents = self.dirents

    def tearDown(self):
        pass

    def test_get_files_list(self):
        file_list = self.dir.get_files_list()
        self.assertIn(self.file_dirent, file_list)
        self.assertIn(self.testmd, file_list)

    def test_get_subdirs_list(self):
        dirs_list = self.dir.get_subdirs_list()
        self.assertIn(self.dir_dirent, dirs_list)

    def test_lookup_dent(self):
        self.assertEqual(self.dir_dirent, self.dir.lookup_dent('folder'))
        self.assertEqual(self.file_dirent, self.dir.lookup_dent('file'))
        self.assertEqual(self.testmd, self.dir.lookup_dent('test.md'))

    def test_remove_entry(self):
        self.assertIn(self.dir_dirent, self.dir.dirents.itervalues())
        self.assertIn(self.file_dirent, self.dir.dirents.itervalues())
        self.dir.remove_entry('folder')
        self.assertNotIn(self.dir_dirent, self.dir.dirents.itervalues())
        self.dir.remove_entry('file')
        self.assertNotIn(self.file_dirent, self.dir.dirents.itervalues())


    @mock.patch('seafobj.fs.SeafFSManager.load_seafdir', side_effect=mock_return_args)
    @mock.patch('seafobj.fs.SeafFSManager.load_seafile', side_effect=mock_return_args)
    def test_lookup(self, fake_load_seafile, fake_load_seadir):
        self.assertEqual(self.dir.lookup('folder'), (self.dir_store_id, 0, self.dir_dirent.id))
        self.assertEqual(self.dir.lookup('test.md'), (self.dir_store_id, 0, self.testmd.id))
        self.assertEqual(self.dir.lookup('file'), (self.dir_store_id, 0, self.file_dirent.id))


class TestSeafFile(BaseTest):
    def setUp(self):
        self.seafile = self.get_seaffile_v0()

    def tearDown(self):
        pass

    def test_init(self):
        self.assertEqual(self.seafile.version, 0)
        self.assertEqual(self.seafile.store_id, self.file_store_id)
        self.assertEqual(self.seafile.obj_id, self.file_obj_id)
        self.assertEqual(len(self.seafile.blocks), 0)
        self.assertEqual(self.seafile.size, self.file_size)

    @mock.patch('seafobj.fs.SeafileStream.read', side_effect=mock_return_args)
    def test_get_content(self, fake_read):
        self.assertEqual(self.seafile.get_content(1), (1,))
        self.assertEqual(self.seafile.get_content(-1), (self.file_size,))

class TestSeafileStream(BaseTest):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_read(self):
        seafile = fs_mgr.load_seafile('db5f5f32-2c1a-429b-9d3b-8063f17f8aa6/', 1, '1f49a59c43df82d5d97d88fef8bca70b93efc299')
        s = SeafileStream(seafile)
        self.assertEqual(s.read(100), 'modified\nzm')

    def test_read_with_multi_thread(self):
        for i in range(10):
            th = threading.Thread(target=self.test_read)
            th.start()

class TestSeafFSManager(BaseTest):
    def setUp(self):
        self.repo_id = 'db5f5f32-2c1a-429b-9d3b-8063f17f8aa6/'
        self.file_blocks = ['124b7a78cecccd30456276dc28aadffbaccc9f31']
        self.file_obj_id = '1f49a59c43df82d5d97d88fef8bca70b93efc299'
        self.dir_obj_id = 'b35df9cf2e9353c20499ed08c4a1b5215975ea76'
        self.dir_subdirent = ['create_has_been_renamed_folder',
                              'create_modified_files.md',
                              'create_new_file.md', 
                              'create_renamed_file.md']


    def tearDown(self):
        pass

    def test_load_seafile(self):
        seafile = fs_mgr.load_seafile(self.repo_id, 1, self.file_obj_id)
        self.assertTrue(isinstance(seafile, SeafFile))
        self.assertEqual(seafile.__dict__, {'blocks': self.file_blocks, 'store_id': self.repo_id, '_content': None, 'obj_id': self.file_obj_id, 'version': 1, 'size': 11})

    def test_load_seafdir(self):
        seafdir = fs_mgr.load_seafdir(self.repo_id, 1, self.dir_obj_id)
        for dirent in self.dir_subdirent:
            self.assertIn(dirent, seafdir.dirents.keys())

    def test_parse_dirents_v0(self):
        dir_binary_data = '\x00\x00\x00\x03\x00\x00@\x000123456789012345678901234567890123456789\x00\x00\x00\x06folder'
        file_binary_data = '\x00\x00\x00\x03\x00\x00\x80\x009876543210987654321098765432109876543210\x00\x00\x00\x04file'
        dir_dirent = fs_mgr.parse_dirents_v0(dir_binary_data, '123123123')
        file_dirent = fs_mgr.parse_dirents_v0(file_binary_data, '312321321')
        self.assertEqual(dir_dirent['folder'].name, 'folder')
        self.assertEqual(dir_dirent['folder'].type, 0)
        self.assertEqual(dir_dirent['folder'].id, '0123456789' * 4)
        self.assertEqual(file_dirent['file'].name, 'file')
        self.assertEqual(file_dirent['file'].type, 1)
        self.assertEqual(file_dirent['file'].id, '9876543210' * 4)
        
    def test_parse_dirents_v1(self):
        json_data = {'dirents':[
            {'name': 'file_1', 'id': 'f' * 40, 'mtime': '1516846158.123137', 'mode': 32768, 'size': 1024},
            {'name': 'makedown', 'id': 'm' * 40, 'mtime': '1516842222.123137', 'mode': 32768, 'size': 1024},
            {'name': 'dir_1', 'id': 'd' * 40, 'mtime': '1516846258.123137', 'mode': 16384, 'size': 0}]}
        res_dirents = fs_mgr.parse_dirents_v1(json.dumps(json_data), 'temp_dir')
        for data in json_data['dirents']:
            keys = data.keys()
            keys.remove('mode')
            for key in keys:
                self.assertEqual(res_dirents[data['name']].__getattribute__(key), data[key])

    def test_parse_blocks_v0(self):
        binary_data = struct.pack('!iq20s20s', 1, 20000, 'a' * 20, 'c' * 20)
        self.assertEqual((['6161616161616161616161616161616161616161', '6363636363636363636363636363636363636363'], 20000),
                         fs_mgr.parse_blocks_v0(binary_data, 'obj_id'))

    def test_parse_blocks_v1(self):
        json_data = {'block_ids': ['x1' * 20, 'yz' * 20, 'lz' * 20], 'size': 200}
        res_data = fs_mgr.parse_blocks_v1(json.dumps(json_data), 'obj_id')
        self.assertEqual(res_data, (['x1' * 20, 'yz'*20, 'lz'*20], 200))
