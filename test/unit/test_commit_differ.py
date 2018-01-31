import threading

from ..base import BaseTest

from seafobj import CommitDiffer, commit_mgr

Success = True


class TestCommitDiffer(BaseTest):
    def setUp(self):
        self.repo_id = self.TEST_CEPH_REPO_ID
        self.repo_version = 1
        self.lst_commit = self.TEST_CEPH_HEAD_COMMIT
        self.fst_commit = self.TEST_CEPH_LAST_COMMIT
        self.add_commit = self.TEST_CEPH_ADD_COMMIT

    def diff(self):
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

        self.assertTrue(len(deleted_files) > 0)
        self.assertEquals(deleted_files[0].path, '/create_deleted_file.md')

        self.assertTrue(len(modified_files) > 0)
        self.assertEqual(modified_files[0].path, '/create_modified_files.md')

        self.assertTrue(len(renamed_files) > 0)
        self.assertEqual(renamed_files[0].path, '/create_renamed_file.md')

        self.assertTrue(len(moved_files) > 0)
        self.assertEqual(moved_files[0].path, '/create_moved_file.md')

        self.assertTrue(len(deleted_dirs) > 0)
        self.assertEquals(deleted_dirs[0].path, '/create_deleted_folder')

        self.assertTrue(len(renamed_dirs) > 0)
        self.assertEquals(renamed_dirs[0].path, '/create_renamed_folder')

        self.assertTrue(len(moved_dirs) > 0)
        self.assertEquals(moved_dirs[0].path, '/create_moved_folder')

        #strs = ['added_files', 'deleted_files', 'added_dirs', 'deleted_dirs', 'modified_files', \
        #'renamed_files', 'moved_files', 'renamed_dirs', 'moved_dirs']
        #for i, v in enumerate(differ.diff()):
        #    print strs[i]
        #    for o in v:
        #        print o.__dict__

    def test_diff(self):
        for i in range(100):
            self.diff()

    def catch_with_diff(self):
        try:
            self.test_diff()
        except AssertionError:
            global Success
            Success = False
            #raise e
        except Exception as e:
            raise e

    def test_diff_with_multi_thread(self):
        ths = []
        for i in range(20):
            th = threading.Thread(target=self.catch_with_diff)
            ths.append(th)
            th.start()
        for th in ths:
            th.join()
        self.assertTrue(Success)
