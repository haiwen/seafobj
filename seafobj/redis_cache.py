import redis

class RedisCache(object):
    def __init__(self, host, port, expiry, max_connections, passwd):
        self.expiry = expiry
        pool = redis.ConnectionPool(host=host, port=port, max_connections=max_connections, password=passwd)
        self.client = redis.StrictRedis(connection_pool=pool)

    def set_obj(self, repo_id, obj_id, value):
        try:
            key = '%s-%s' % (repo_id, obj_id)
            self.client.set(key, value, ex=self.expiry)
        except Exception:
            return

    def get_obj(self, repo_id, obj_id):
        try:
            key = '%s-%s' % (repo_id, obj_id)
            data = self.client.get(key)
            return data
        except Exception:
            return None

def get_redis_cache(host, port, expiry, max_connections, passwd):
    return RedisCache(host, port, expiry, max_connections, passwd)
