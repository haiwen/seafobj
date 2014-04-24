from .objstore_factory import objstore_factory
from seafobj.utils import to_utf8

try:
    import json
except ImportError:
    import simplejson as json

class SeafCommit(object):
    def __init__(self, _dict):
        self._dict = _dict

    def __getattr__(self, key):
        if key in self._dict:
            return self._dict[key]
        return object.__getattribute__(self, key)
        
    def get_version(self):
        return self._dict.get('version', 0)

class SeafCommitManager(object):
    def __init__(self):
        self.obj_store = objstore_factory.get_obj_store('commits')
        self._counter = 0
        
    def read_count(self):
        return self._counter

    def load_commit(self, repo_id, version, obj_id):
        self._counter += 1
        data = self.obj_store.read_obj(repo_id, version, obj_id)
        return self.parse_commit(data)

    def parse_commit(self, data):
        dict = json.loads(data)
        d = {}
        for k, v in dict.iteritems():
            d[to_utf8(k)] = v
        return SeafCommit(d)

    def is_commit_encrypted(self, repo_id, version, commit_id):
        commit = self.load_commit(repo_id, version, commit_id)
        return getattr(commit, 'encrypted', False)

    def get_commit_root_id(self, repo_id, version, commit_id):
        commit = self.load_commit(repo_id, version, commit_id)
        return commit.root_id
        
    def get_backend_name(self):
        return self.obj_store.get_name()


commit_mgr = SeafCommitManager()