import os
import unittest
import threading
from os.path import join, dirname, abspath

from seafobj import CommitDiffer, commit_mgr
from seafobj.commit_differ import make_path

TEST_REPO_ID = 'db5f5f32-2c1a-429b-9d3b-8063f17f8aa6'
TEST_HEAD_COMMIT = '09ba1bac75dc9ac993eb6cdb183890005a7fd133'
TEST_LAST_COMMIT = 'e1e5637133317b78409116bb2980dca882296435'
TEST_ADD_COMMIT = '0fad7f0baa7381cb9367a4c5ba9e4ec07d347b27'


class TestCommitDiffer(unittest.TestCase):
    def setUp(self):
        self.repo_id = TEST_REPO_ID
        self.repo_version = 1
        self.lst_commit = TEST_HEAD_COMMIT
        self.fst_commit = TEST_LAST_COMMIT
        self.add_commit = TEST_ADD_COMMIT

    def test_diff(self):
        commit = commit_mgr.load_commit(self.repo_id, self.repo_version, self.add_commit)
        parent = commit_mgr.load_commit(self.repo_id, commit.version, self.fst_commit)
        differ = CommitDiffer(self.repo_id, commit.version, parent.root_id, commit.root_id, True, True)

        added_files, deleted_files, added_dirs, deleted_dirs, modified_files, \
        renamed_files, moved_files, renamed_dirs, moved_dirs = differ.diff()
        added_file_names = ['create_new_file.md', 'create_renamed_file.md',
                            'create_moved_file.md', 'create_deleted_file.md',
                            'create_modified_files.md']
        added_folder_names = ['create_added_folder', 'create_moved_folder',
                              'create_deleted_folder', 'create_renamed_folder']

        all_files_names = [f.path for f in added_files]
        all_folder_names = [f.path for f in added_dirs]
        for f in added_file_names:
            self.assertIn('/' + f, all_files_names)
        for f in added_folder_names:
            self.assertIn('/' + f, all_folder_names)

        commit = commit_mgr.load_commit(self.repo_id, self.repo_version, self.lst_commit)
        parent = commit_mgr.load_commit(self.repo_id, commit.version, self.add_commit)
        differ = CommitDiffer(self.repo_id, commit.version, parent.root_id, commit.root_id, True, True)

        added_files, deleted_files, added_dirs, deleted_dirs, modified_files, \
        renamed_files, moved_files, renamed_dirs, moved_dirs = differ.diff()

        self.assertEquals(deleted_files[0].path, '/create_deleted_file.md')
        self.assertEquals(added_dirs[0].path, '/create_has_been_renamed_folder')
        deleted_dir_names = ['/create_renamed_folder', '/create_deleted_folder', '/create_moved_folder']
        for name in deleted_dir_names:
            self.assertIn(name, [d.path for d in deleted_dirs])
        self.assertEqual(modified_files[0].path, '/create_modified_files.md')
        self.assertEqual(renamed_files[0].path, '/create_renamed_file.md')
        self.assertEqual(moved_files[0].path, '/create_moved_file.md')

        ## because some file and dir move to other dir, so this dir obj_id will be changed,
        ## then diff operation will be worry if some operation in this dir.
        ## This operation occurs only when get diff of two commit that range more than two minimum commit.
        ## And it's right.

        ## after update obj
        commit = commit_mgr.load_commit(self.repo_id, self.repo_version, self.lst_commit)
        parent = commit_mgr.load_commit(self.repo_id, commit.version, '9aebb82af0bfc01b9dc22637cc1a60535cb871d3')
        differ = CommitDiffer(self.repo_id, commit.version, parent.root_id, commit.root_id, True, True)

        added_files, deleted_files, added_dirs, deleted_dirs, modified_files, \
        renamed_files, moved_files, renamed_dirs, moved_dirs = differ.diff()

        self.assertEqual(moved_dirs[0].path, '/create_moved_folder')
        self.assertEqual(moved_dirs[0].new_path, '/create_has_been_renamed_folder/create_moved_folder')

        commit = commit_mgr.load_commit(self.repo_id, self.repo_version, '9aebb82af0bfc01b9dc22637cc1a60535cb871d3')
        parent = commit_mgr.load_commit(self.repo_id, commit.version, '1463e97264c6fa2f6f97804ab778a11df5bebc7f')
        differ = CommitDiffer(self.repo_id, commit.version, parent.root_id, commit.root_id, True, True)

        added_files, deleted_files, added_dirs, deleted_dirs, modified_files, \
        renamed_files, moved_files, renamed_dirs, moved_dirs = differ.diff()

        #strs = ['added_files', 'deleted_files', 'added_dirs', 'deleted_dirs', 'modified_files', \
        #'renamed_files', 'moved_files', 'renamed_dirs', 'moved_dirs']
        #for i, v in enumerate(differ.diff()):
        #    print strs[i]
        #    for o in v:
        #        print o.__dict__
