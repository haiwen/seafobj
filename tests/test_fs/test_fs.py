import threading
import pytest
from seafobj import fs_mgr

class TestFSManager():
    def __init__(self):
        self.repo_id = '3f9e4aa5-d6ba-4066-a1d6-81824f422af1'
        self.first_dir_id = 'b27235c5278ccba69b392fa1734138bf4100693c'
        self.second_dir_id = '3ef00cc2e396b7492eaee6e7d0fc6e23279569c5'
        self.first_file_id = '9771cd218f1002e59c6f0dc6ee2dc57dc9dde698'
        self.second_file_id = '67619f6d87f2f232bb0a821649f9fd1358eaa58c'

def load_seafdir():
    mgr = TestFSManager()
    seafdir = fs_mgr.load_seafdir(mgr.repo_id, 1, mgr.first_dir_id)
    assert mgr.first_dir_id == seafdir.obj_id
    assert 'create_moved_folder' in list(seafdir.dirents.keys())
    assert 'create_moved_file.md' in list(seafdir.dirents.keys())
    assert seafdir.dirents.get('create_moved_file.md', None).name == 'create_moved_file.md'
    assert '045dfc08495b5c6cbc1a4dc347f5e2987fd809f4' == seafdir.dirents['create_moved_file.md'].id
    assert seafdir.dirents.get('create_moved_folder', None).name == 'create_moved_folder'
    assert '05a6f0455d1f11ecfc202f5e218274b092fd3dbc' == seafdir.dirents['create_moved_folder'].id
    seafdir = fs_mgr.load_seafdir(mgr.repo_id, 1, mgr.second_dir_id)
    assert 'added_folder.md' in list(seafdir.dirents.keys())
    assert mgr.second_dir_id == seafdir.obj_id


def test_load_seafdir():
    for i in range(100):
        load_seafdir()

Success = True
def catch_with_load_seafdir():
    try:
        test_load_seafdir()
    except Exception as e:
        global Success
        Success = False
        raise e

def test_load_seafdir_with_mutli_thread():
    global Success
    Success = True
    ths = []
    for i in range(20):
        th = threading.Thread(target=catch_with_load_seafdir)
        ths.append(th)
        th.start()
    for th in ths:
        th.join()
    assert Success == True

def load_seafile():
    mgr = TestFSManager()
    seafile = fs_mgr.load_seafile(mgr.repo_id, 1, mgr.first_file_id)
    assert len(seafile.blocks) > 0
    assert '2949afb5a9c351b9415b91c8f3d0d98991118c11' == seafile.blocks[0]
    second_seafile = fs_mgr.load_seafile(mgr.repo_id, 1, mgr.second_file_id)
    assert 1 == len(second_seafile.blocks)
    assert '125f1e9dc9f3eca5a6819f9b4a2e17e53d7e2f78' == second_seafile.blocks[0]

def test_load_seafile():
    for i in range(100):
        load_seafile()

def catch_with_load_seafile():
    try:
        test_load_seafile()
    except Exception as e:
        global Success
        Success = False
        raise e

def test_load_seafile_with_multi_thread():
    global Success
    Success = True
    ths = []
    for i in range(20):
        th = threading.Thread(target=catch_with_load_seafile)
        ths.append(th)
        th.start()
    for th in ths:
        th.join()
    assert Success == True
