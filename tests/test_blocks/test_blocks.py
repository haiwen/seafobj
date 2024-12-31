import threading
import pytest
from seafobj import block_mgr

class TestBlockManager():
    def __init__(self):
        self.repo_id = '3f9e4aa5-d6ba-4066-a1d6-81824f422af1'
        self.modified_bkid = '125f1e9dc9f3eca5a6819f9b4a2e17e53d7e2f78'
        self.new_bkid = '2949afb5a9c351b9415b91c8f3d0d98991118c11'
        self.renamed_bkid = 'b73b3cf6dc021d20c7a0e9bedf46a5b6a58bdd53'
        self.moved_bkid = '1569cf662c7befe4c4891a22cc7a1c035bc8bfac'

def load_block():
    mgr = TestBlockManager()
    seafblk = block_mgr.load_block(mgr.repo_id, 1, mgr.new_bkid)
    assert b'this is new file.' in seafblk
    seafblk = block_mgr.load_block(mgr.repo_id, 1, mgr.modified_bkid)
    assert b'this is modified file' in seafblk
    seafblk = block_mgr.load_block(mgr.repo_id, 1, mgr.renamed_bkid)
    assert b'this is renamed file.' in seafblk
    seafblk = block_mgr.load_block(mgr.repo_id, 1, mgr.moved_bkid)
    assert b'this is moved file.' in seafblk

def test_load_block ():
    for i in range(100):
        load_block()

Success = True
def catch_with_load_block():
    try:
        test_load_block()
    except Exception as e:
        global Success
        Success = False
        raise e

def test_load_block_with_multi_thread():
    ths = []
    for i in range(20):
        th = threading.Thread(target=catch_with_load_block)
        ths.append(th)
        th.start()
    for th in ths:
        th.join()
    assert Success == True
