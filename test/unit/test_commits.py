import os
import unittest
import threading
from os.path import join, dirname, abspath

from seafobj import commit_mgr

TEST_REPO_ID = '413c175b-0f7d-4616-8298-22bc147af43c'
TEST_HEAD_COMMIT = '2b216582a86ca7ab72264c3936350363a79c6d23'


class TestCommits(unittest.TestCase):
    def setUp(self):
        self.repo_id = TEST_REPO_ID
        self.repo_version = 1
        self.commit_id = TEST_HEAD_COMMIT

    def load_commit(self):
        commit = commit_mgr.load_commit(self.repo_id, self.repo_version, self.commit_id)
        self.assertEquals(commit.commit_id, self.commit_id)
        self.assertEquals(commit.repo_id, self.repo_id)
        self.assertEquals(commit.repo_name, 'backends-test')
        attrs = ['commit_id', 'repo_id', 'description', 'creator_name',
                 'creator', 'root_id', 'parent_id', 'second_parent_id',
                 'version', 'no_local_history', 'repo_desc', 'repo_category',
                 'repo_name', 'ctime']
        for attr in attrs:
            self.assertIn(attr, commit.__dict__['_dict'].keys())

    def test_can_load_commit(self):
        self.load_commit()

    def test_can_load_commit_with_multi(self):
        ths = []
        for i in range(10):
            th = threading.Thread(target=self.load_commit, name='test_'+str(i))
            th.start()
            ths.append(th)
        for th in ths:
            th.join()
