import os
import shutil
import random
import threading

from ...base import BaseTest
from ...utils import random_string
from seafobj.backends.filesystem import SeafObjStoreFS


class TestSeafObjStoreFS(BaseTest):
    def setUp(self):
        self.commit_dir = os.path.join(self.top_dir, 'commits')
        self.repo_id = random_string(36)
        self.obj_id = random_string(40)
        self.storeFS = SeafObjStoreFS(False, self.commit_dir)
        self.path = os.path.join(self.top_dir, 'commits', self.repo_id, self.obj_id[:2], self.obj_id[2:])
        if not os.path.exists(os.path.dirname(self.path)):
            os.makedirs(os.path.dirname(self.path))
        self.obj_id_list = [random_string(40) for i in range(10)]
        self.other_repo_id = random_string(36)

    def tearDown(self):
        shutil.rmtree(os.path.join(self.top_dir, 'commits', self.repo_id))

    def test_read_obj_raw(self):
        with open(self.path, 'w') as fd:
            fd.write('temp file')
        data = self.storeFS.read_obj_raw(self.repo_id, 1, self.obj_id)
        self.assertEqual(data, 'temp file')

    def test_read_obj(self):
        with open(self.path, 'w') as fd:
            fd.write('temp file')
        data = self.storeFS.read_obj(self.repo_id, 1, self.obj_id)
        self.assertEqual(data, 'temp file')

    def read_obj_task(self, mapping):
        repo_id = random.choice(mapping.keys())
        obj_id_list = mapping[repo_id].keys()
        for obj_id in obj_id_list:
            data = self.storeFS.read_obj_raw(repo_id, 1, obj_id)
            self.assertEqual(data, mapping[repo_id][obj_id])

    def test_read_obj_by_multi_thread(self):
        ## write data
        mapping = {}
        for i in range(5):
            repo_id = random_string(36)
            mapping[repo_id] = {}
            obj_id_list = [random_string(40) for i in range(20)]
            for obj_id in obj_id_list:
                data = random_string(20)
                mapping[repo_id][obj_id] = data
                self.write_obj(data, 'commits', repo_id, obj_id)

        ## read data by multi thread
        ths = []
        for i in range(20):
            th = threading.Thread(target=self.read_obj_task, args=(mapping, ))
            ths.append(th)
            th.start()
        for th in ths:
            th.join()

        # clear
        for repo_id in mapping.keys():
            shutil.rmtree(os.path.join(self.top_dir, 'commits', repo_id))

    def test_list_objs(self):
        for obj_id in self.obj_id_list:
            self.write_obj(" all right ", 'commits', self.repo_id, obj_id)

        obj_list = [obj[1] for obj in self.storeFS.list_objs() if obj[0] == self.repo_id]
        for obj in obj_list:
            self.assertIn(obj, self.obj_id_list)

    def test_obj_exists(self):
        for obj_id in self.obj_id_list:
            self.write_obj(" all right ", 'commits', self.repo_id, obj_id)

        for obj_id in self.obj_id_list:
            self.assertTrue(self.storeFS.obj_exists(self.repo_id, obj_id))

    def test_write_obj(self):
        for obj_id in self.obj_id_list:
            self.storeFS.write_obj('hello', self.repo_id, obj_id)
        for obj_id in self.obj_id_list:
            tmp_path = os.path.join(self.top_dir, 'commits', self.repo_id, obj_id[:2], obj_id[2:])
            with open(tmp_path) as fd:
                self.assertEqual('hello', fd.read())
