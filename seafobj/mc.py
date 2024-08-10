import pylibmc
import re

class McCache(object):
    def __init__(self, mc_options):
        self.server = 'localhost'
        self.port = 11211
        self.parse_mc_options(mc_options)
        address = f"{self.server}:{self.port}"
        self.client = pylibmc.Client([address], binary=True, behaviors={"tcp_nodelay": True,
                                                                        "ketama": True})

    def parse_mc_options(self, mc_options):
        match = re.match('--SERVER\\s*=\\s*(\d+\.\d+\.\d+\.\d+):(\d+)', mc_options)
        if match:
            self.server = match.group(1)
            port = match.group(2)
            if port:
                self.port = int(port)

    def set_obj(self, repo_id, obj_id, value):
        key = '%s-%s' % (repo_id, obj_id)
        self.client.set(key, value, time=24*3600)

    def get_obj(self, repo_id, obj_id):
        key = '%s-%s' % (repo_id, obj_id)
        return self.client.get(key)

def get_mc_cache(mc_options):
    return McCache(mc_options)
