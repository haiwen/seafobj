import pylibmc
from pylibmc import ClientPool
import re

class McCache(object):
    def __init__(self, mc_options):
        self.server = 'localhost'
        self.port = 11211
        self.parse_mc_options(mc_options)
        address = f"{self.server}:{self.port}"
        client = pylibmc.Client([address], behaviors={"tcp_nodelay": True})
        self.pool = ClientPool(client, 20)

    def parse_mc_options(self, mc_options):
        match = re.match('--SERVER\\s*=\\s*(\d+\.\d+\.\d+\.\d+):(\d+)', mc_options)
        if match:
            self.server = match.group(1)
            port = match.group(2)
            if port:
                self.port = int(port)

    def set_obj(self, repo_id, obj_id, value):
        try:
            key = '%s-%s' % (repo_id, obj_id)
            with self.pool.reserve() as client:
                client.set(key, value, time=24*3600)
        except Exception:
            return

    def get_obj(self, repo_id, obj_id):
        try:
            key = '%s-%s' % (repo_id, obj_id)
            with self.pool.reserve() as client:
                data = client.get(key)
                return data
            return None
        except Exception:
            return None

def get_mc_cache(mc_options):
    return McCache(mc_options)
