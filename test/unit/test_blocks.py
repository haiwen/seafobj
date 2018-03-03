import threading

from ..base import BaseTest

from seafobj import block_mgr

Success = True


class TestSeafBlockManager(BaseTest):
    def setUp(self):
        self.repo_id = self.TEST_CEPH_REPO_ID
        self.repo_id_2 = self.TEST_CEPH_REPO_ID_2
        self.modified_bkid = '125f1e9dc9f3eca5a6819f9b4a2e17e53d7e2f78'
        self.new_bkid = '2949afb5a9c351b9415b91c8f3d0d98991118c11'
        self.renamed_bkid = 'b73b3cf6dc021d20c7a0e9bedf46a5b6a58bdd53'
        self.moved_bkid = '1569cf662c7befe4c4891a22cc7a1c035bc8bfac'

    def load_block(self):
        seafblk = block_mgr.load_block(self.repo_id, 1, self.new_bkid)
        self.assertIn('this is new file.', seafblk)
        seafblk = block_mgr.load_block(self.repo_id, 1, self.modified_bkid)
        self.assertIn('this is modified file', seafblk)
        seafblk = block_mgr.load_block(self.repo_id, 1, self.renamed_bkid)
        self.assertIn('this is renamed file.', seafblk)
        seafblk = block_mgr.load_block(self.repo_id, 1, self.moved_bkid)
        self.assertIn('this is moved file.', seafblk)

    def load_block_2(self):
        seafblk = block_mgr.load_block(self.repo_id_2, 1, self.new_bkid)
        self.assertIn('this is new file.', seafblk)
        seafblk = block_mgr.load_block(self.repo_id_2, 1, self.modified_bkid)
        self.assertIn('this is modified file', seafblk)
        seafblk = block_mgr.load_block(self.repo_id_2, 1, self.renamed_bkid)
        self.assertIn('this is renamed file.', seafblk)
        seafblk = block_mgr.load_block(self.repo_id_2, 1, self.moved_bkid)
        self.assertIn('this is moved file.', seafblk)

    def test_load_block(self):
        test_multi = True
        try:
            obj_stores = block_mgr.obj_stores
        except AttributeError:
            test_multi = False

        if test_multi:
            for i in range(100):
                self.load_block()
                self.load_block_2()
        else:
            for i in range(100):
                self.load_block()

    def catch_with_load_block(self):
        try:
            self.test_load_block()
        except AssertionError:
            global Success
            Success = False
            #raise e
        except Exception as e:
            raise e

    def test_load_block_with_multi_thread(self):
        ths = []
        for i in range(20):
            th = threading.Thread(target=self.test_load_block)
            ths.append(th)
            th.start()
        for th in ths:
            th.join()
        self.assertTrue(Success)
