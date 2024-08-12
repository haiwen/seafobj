import redis

class RedisCache(object):
    def __init__(self, host, port, expiry, max_connections):
        self.expiry = expiry
        pool = redis.ConnectionPool(host=host, port=port, max_connections=max_connections)
        self.client = redis.StrictRedis(connection_pool=pool)

    def set_obj(self, repo_id, obj_id, value):
        key = '%s-%s' % (repo_id, obj_id)
        self.client.set(key, value, ex=self.expiry)

    def get_obj(self, repo_id, obj_id):
        key = '%s-%s' % (repo_id, obj_id)
        return self.client.get(key)

def get_redis_cache(host, port, expiry, max_connections):
    return RedisCache(host, port, expiry, max_connections)
