import threading
import pytest
from seafobj import commit_mgr
from seafobj.commits import SeafCommit

class TestCommitManager():
    def __init__(self):
        self.repo_id = '3f9e4aa5-d6ba-4066-a1d6-81824f422af1'
        self.last_commit = '4405b7234b1e9dd74fe7c4f6a844ce79198e0e19'
        self.head_commit = 'a24bf4385e5df18922337390e757c4b7789d853d'

def load_commits():
    mgr = TestCommitManager()
    seafcmt = commit_mgr.load_commit(mgr.repo_id, 1, mgr.head_commit)
    assert isinstance(seafcmt, SeafCommit) == True
    assert 'Renamed directory "create_renamed_folder"' == seafcmt.description
    assert 'ffc32568c059e9532cb426f19f8138c624c5cdd4' == seafcmt.parent_id
    assert 'obj_test' == seafcmt.repo_name
    assert 1517211913 == seafcmt.ctime
    seafcmt = commit_mgr.load_commit(mgr.repo_id, 1, mgr.last_commit)
    assert 'Modified "added_folder.md"' == seafcmt.description
    assert '9e4705d102d86756eb8ed9d8d16922ee3212c7c5' == seafcmt.parent_id
    assert 'obj_test' == seafcmt.repo_name
    assert 1517211712 == seafcmt.ctime

def test_load_commit():
    for i in range(100):
        load_commits()

Success = True
def catch_with_load_commits():
    try:
        test_load_commit()
    except Exception as e:
        global Success
        Success = False
        raise e

def test_load_commit_with_multi_thread():
    ths = []
    for i in range(20):
        th = threading.Thread(target=catch_with_load_commits)
        ths.append(th)
        th.start()
    for th in ths:
        th.join()
    assert Success == True
