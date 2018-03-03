import threading

from ..base import BaseTest

from seafobj import commit_mgr
from seafobj.commits import SeafCommit

Success = True


class TestSeafCommitManager(BaseTest):
    def setUp(self):
        self.repo_id = self.TEST_CEPH_REPO_ID
        self.repo_id_2 = self.TEST_CEPH_REPO_ID_2
        self.head_commit = self.TEST_CEPH_HEAD_COMMIT
        self.last_commit = self.TEST_CEPH_ADD_COMMIT

    def load_commits(self):
        seafcmt = commit_mgr.load_commit(self.repo_id, 1, self.head_commit)
        self.assertTrue(isinstance(seafcmt, SeafCommit))
        self.assertEqual('Renamed directory "create_renamed_folder"', seafcmt.description)
        self.assertEqual('ffc32568c059e9532cb426f19f8138c624c5cdd4', seafcmt.parent_id)
        self.assertEqual('obj_test', seafcmt.repo_name)
        self.assertEqual(1517211913, seafcmt.ctime)
        seafcmt = commit_mgr.load_commit(self.repo_id, 1, self.last_commit)
        self.assertEqual('Modified "added_folder.md"', seafcmt.description)
        self.assertEqual('9e4705d102d86756eb8ed9d8d16922ee3212c7c5', seafcmt.parent_id)
        self.assertEqual('obj_test', seafcmt.repo_name)
        self.assertEqual(1517211712, seafcmt.ctime)

    def load_commits_2(self):
        seafcmt = commit_mgr.load_commit(self.repo_id_2, 1, self.head_commit)
        self.assertTrue(isinstance(seafcmt, SeafCommit))
        self.assertEqual('Renamed directory "create_renamed_folder"', seafcmt.description)
        self.assertEqual('ffc32568c059e9532cb426f19f8138c624c5cdd4', seafcmt.parent_id)
        self.assertEqual('obj_test', seafcmt.repo_name)
        self.assertEqual(1517211913, seafcmt.ctime)
        seafcmt = commit_mgr.load_commit(self.repo_id_2, 1, self.last_commit)
        self.assertEqual('Modified "added_folder.md"', seafcmt.description)
        self.assertEqual('9e4705d102d86756eb8ed9d8d16922ee3212c7c5', seafcmt.parent_id)
        self.assertEqual('obj_test', seafcmt.repo_name)
        self.assertEqual(1517211712, seafcmt.ctime)

    def test_load_commit(self):
        test_multi = True
        try:
            obj_stores = commit_mgr.obj_stores
        except AttributeError:
            test_multi = False

        if test_multi:
            for i in range(100):
                self.load_commits()
                self.load_commits_2()
        else:
            for i in range(100):
                self.load_commits()

    def catch_with_commits(self):
        try:
            self.test_load_commit()
        except AssertionError:
            global Success
            Success = False
            #raise e
        except Exception as e:
            raise e


    def test_load_commit_with_multi_thread(self):
        ths = []
        for i in range(20):
            th = threading.Thread(target=self.catch_with_commits)
            ths.append(th)
            th.start()
        for th in ths:
            th.join()
        self.assertTrue(Success)
