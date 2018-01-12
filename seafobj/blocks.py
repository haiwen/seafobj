from .objstore_factory import objstore_factory

class SeafBlockManager(object):
    def __init__(self):
        self.obj_store = objstore_factory.get_obj_store('blocks')
        self._counter = 0

    def read_count(self):
        return self._counter

    def load_block(self, repo_id, version, obj_id):
        self._counter += 1
        data = self.obj_store.read_obj(repo_id, version, obj_id)
        return data


block_mgr = SeafBlockManager()