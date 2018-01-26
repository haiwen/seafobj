import threading

from ..base import BaseTest

from seafobj import fs_mgr

Success = True


class TestSeafFSManager(BaseTest):
    def setUp(self):
        self.repo_id = self.TEST_CEPH_REPO_ID
        self.first_did = 'b27235c5278ccba69b392fa1734138bf4100693c'
        self.second_did = '3ef00cc2e396b7492eaee6e7d0fc6e23279569c5'
        self.first_fid = '9771cd218f1002e59c6f0dc6ee2dc57dc9dde698'
        self.second_fid = '67619f6d87f2f232bb0a821649f9fd1358eaa58c'

    def load_seafdir(self):
        seafdir = fs_mgr.load_seafdir(self.repo_id, 1, self.first_did)
        self.assertEqual(self.first_did, seafdir.obj_id)
        self.assertIn('create_moved_folder', seafdir.dirents.keys())
        self.assertIn('create_moved_file.md', seafdir.dirents.keys())
        self.assertTrue(seafdir.dirents.get('create_moved_file.md', None))
        self.assertEqual('045dfc08495b5c6cbc1a4dc347f5e2987fd809f4', seafdir.dirents['create_moved_file.md'].id)
        self.assertTrue(seafdir.dirents.get('create_moved_folder', None))
        self.assertEqual('05a6f0455d1f11ecfc202f5e218274b092fd3dbc', seafdir.dirents['create_moved_folder'].id)
        seafdir = fs_mgr.load_seafdir(self.repo_id, 1, self.second_did)
        self.assertIn('added_folder.md', seafdir.dirents.keys())
        self.assertEqual(self.second_did, seafdir.obj_id)

    def test_load_seafdir(self):
        for i in range(100):
            self.load_seafdir()

    def catch_with_load_seafdir(self):
        try:
            self.test_load_seafdir()
        except AssertionError:
            global Success
            Success = False
            #raise e
        except Exception as e:
            raise e

    def test_load_seafdir_with_mutli_thread(self):
        global Success
        Success = True
        ths = []
        for i in range(20):
            th = threading.Thread(target=self.catch_with_load_seafdir)
            ths.append(th)
            th.start()
        for th in ths:
            th.join()
        self.assertTrue(Success)

    def load_seafile(self):
        seafile = fs_mgr.load_seafile(self.repo_id, 1, self.first_fid)
        self.assertEqual(1, len(seafile.blocks))
        self.assertTrue(len(seafile.blocks) > 0)
        self.assertEqual('2949afb5a9c351b9415b91c8f3d0d98991118c11', seafile.blocks[0])
        second_seafile = fs_mgr.load_seafile(self.repo_id, 1, self.second_fid)
        self.assertEqual(1, len(second_seafile.blocks))
        self.assertTrue(len(second_seafile.blocks) > 0)
        self.assertEqual('125f1e9dc9f3eca5a6819f9b4a2e17e53d7e2f78', second_seafile.blocks[0])

    def test_load_seafile(self):
        for i in range(100):
            self.load_seafile()

    def catch_with_load_seafile(self):
        try:
            self.test_load_seafile()
        except AssertionError:
            global Success
            Success = False
            #raise e
        except Exception as e:
            raise e

    def test_load_seafile_with_multi_thread(self):
        global Success
        Success = True
        ths = []
        for i in range(20):
            th = threading.Thread(target=self.catch_with_load_seafile)
            ths.append(th)
            th.start()
        for th in ths:
            th.join()
        self.assertTrue(Success)
