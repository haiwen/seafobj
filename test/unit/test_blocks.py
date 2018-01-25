import os
import shutil

from ..base import BaseTest
from ..utils import random_string

from seafobj.blocks import SeafBlockManager


class TestSeafBlockManager(BaseTest):
    def setUp(self):
        self.block_path = os.path.join(self.top_dir, 'blocks')
        self.repo_id = random_string(36)
        self.obj_count = 10
        self.obj_id_list = [random_string(40) for i in range(self.obj_count)]
        self.block_manager = SeafBlockManager()

    def tearDown(self):
        shutil.rmtree(os.path.join(self.block_path, self.repo_id))

    def test_load_block(self):
        mapping = {}
        for obj_id in self.obj_id_list:
            tmp_data = random_string(10)
            mapping[obj_id] = tmp_data
            self.write_obj(tmp_data, 'blocks', self.repo_id, obj_id)

        for obj_id in self.obj_id_list:
            self.assertEqual(self.block_manager.load_block(self.repo_id, 1, obj_id), mapping[obj_id])
