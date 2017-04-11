#coding: UTF-8

from seafobj import fs_mgr
import os
import logging

ZERO_OBJ_ID = '0000000000000000000000000000000000000000'

class DiffEntry(object):
    def __init__(self, path, obj_id, size=-1, new_path=None):
        self.path = path
        self.new_path = new_path
        self.obj_id = obj_id
        self.size = size

class CommitDiffer(object):
    def __init__(self, repo_id, version, root1, root2, handle_rename=False, fold_dirs=False):
        self.repo_id = repo_id
        self.version = version
        self.root1 = root1
        self.root2 = root2
        self.handle_rename = handle_rename
        self.fold_dirs = fold_dirs

    def diff(self):
        added_files = []
        deleted_files = []
        deleted_dirs = []
        modified_files = []
        added_dirs = []
        renamed_files = []
        renamed_dirs = []
        moved_files = []
        moved_dirs = []

        new_dirs = []
        queued_dirs = [] # (path, dir_id1, dir_id2)

        if self.root1 == self.root2:
            return (added_files, deleted_files, added_dirs, deleted_dirs,
                    modified_files, renamed_files, moved_files,
                    renamed_dirs, moved_dirs)
        else:
            queued_dirs.append(('/', self.root1, self.root2))

        while True:
            path = old_id = new_id = None
            try:
                path, old_id, new_id = queued_dirs.pop(0)
            except IndexError:
                break

            dir1 = fs_mgr.load_seafdir(self.repo_id, self.version, old_id)
            dir2 = fs_mgr.load_seafdir(self.repo_id, self.version, new_id)

            for dent in dir1.get_files_list():
                new_dent = dir2.lookup_dent(dent.name)
                if not new_dent or new_dent.type != dent.type:
                    deleted_files.append(DiffEntry(make_path(path, dent.name), dent.id, dent.size))
                else:
                    dir2.remove_entry(dent.name)
                    if new_dent.id == dent.id:
                        pass
                    else:
                        modified_files.append(DiffEntry(make_path(path, dent.name), new_dent.id, new_dent.size))

            added_files.extend([DiffEntry(make_path(path, dent.name), dent.id, dent.size) for dent in dir2.get_files_list()])

            for dent in dir1.get_subdirs_list():
                new_dent = dir2.lookup_dent(dent.name)
                if not new_dent or new_dent.type != dent.type:
                    deleted_dirs.append(DiffEntry(make_path(path, dent.name), dent.id))
                else:
                    dir2.remove_entry(dent.name)
                    if new_dent.id == dent.id:
                        pass
                    else:
                        queued_dirs.append((make_path(path, dent.name), dent.id, new_dent.id))

            new_dirs.extend([DiffEntry(make_path(path, dent.name), dent.id) for dent in dir2.get_subdirs_list()])

        if not self.fold_dirs:
            while True:
                # Process newly added dirs and its sub-dirs, all files under
                # these dirs should be marked as added.
                path = obj_id = None
                try:
                    dir_dent = new_dirs.pop(0)
                    added_dirs.append(DiffEntry(dir_dent.path, dir_dent.obj_id))
                except IndexError:
                    break
                d = fs_mgr.load_seafdir(self.repo_id, self.version, dir_dent.obj_id)
                added_files.extend([DiffEntry(make_path(path, dent.name), dent.id, dent.size) for dent in d.get_files_list()])

                added_dirs.extend([DiffEntry(make_path(path, dent.name), dent.id) for dent in d.get_subdirs_list()])
        else:
            added_dirs = new_dirs

        if self.handle_rename:
            ret_added_files = []
            ret_added_dirs = []

            del_file_dict = {de.obj_id: de for de in deleted_files}
            for de in added_files:
                if de.obj_id in del_file_dict:
                    del_de = del_file_dict[de.obj_id]
                    if os.path.dirname(de.path) == os.path.dirname(del_de.path):
                        # it's a rename operation if add and del are in the same dir
                        renamed_files.append(DiffEntry(del_de.path, de.obj_id, de.size, de.path))
                    else:
                        moved_files.append(DiffEntry(del_de.path, de.obj_id, de.size, de.path))
                    del del_file_dict[de.obj_id]
                else:
                    ret_added_files.append(de)

            del_dir_dict = {de.obj_id: de for de in deleted_dirs}
            for de in added_dirs:
                if de.obj_id in del_dir_dict:
                    del_de = del_dir_dict[de.obj_id]
                    if os.path.dirname(de.path) == os.path.dirname(del_de.path):
                        renamed_dirs.append(DiffEntry(del_de.path, de.obj_id, -1, de.path))
                    else:
                        moved_dirs.append(DiffEntry(del_de.path, de.obj_id, -1, de.path))
                    del del_dir_dict[de.obj_id]
                else:
                    ret_added_dirs.append(de)

            added_files = ret_added_files
            added_dirs = ret_added_dirs
            deleted_files = del_file_dict.values()
            deleted_dirs = del_dir_dict.values()

        return (added_files, deleted_files, added_dirs, deleted_dirs,
                modified_files, renamed_files, moved_files,
                renamed_dirs, moved_dirs)

def make_path(dirname, filename):
    if dirname == '/':
        return dirname + filename
    else:
        return '/'.join((dirname, filename))
