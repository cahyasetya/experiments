local key = KEYS[1]
return redis.call("LRANGE", key, 0, -1)
