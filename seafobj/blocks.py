from .objstore_factory import objstore_factory
from .objstore_factory import get_repo_storage_id

class SeafBlockManager(object):
    def __init__(self):
        if not objstore_factory.enable_storage_classes:
            self.obj_store = objstore_factory.get_obj_store('blocks')
        else:
            self.obj_stores = objstore_factory.get_obj_stores('blocks')
        self._counter = 0

    def read_count(self):
        return self._counter

    def load_block(self, repo_id, version, obj_id):
        self._counter += 1
        if not objstore_factory.enable_storage_classes:
            data = self.obj_store.read_obj(repo_id, version, obj_id)
        else:
            storage_id = get_repo_storage_id(repo_id)
            if storage_id:
                data = self.obj_stores[storage_id].read_obj(repo_id, version, obj_id)
            else:
                data = self.obj_stores['__default__'].read_obj(repo_id, version, obj_id)
        return data


block_mgr = SeafBlockManager()
