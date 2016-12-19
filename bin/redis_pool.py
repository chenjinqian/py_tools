# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
redis database connection pooling
Example:
    dic = {'host': '172.16.5.91'}
    redis_pool = RedisWrapper(**dic)
    r = redis_pool.get_cursor()
"""

class RedisWrapper(object):

    def __init__(self, host='127.0.0.1', port='6379', db='0', name=None, **dic):
        "docstring"
        self.host=host
        self.port=int(port)
        self.db=int(db)
        self.dic = dic
        if name is not None: self.name = str(name)

    def get_cursor(self):
        self.pool = redis.ConnectionPool(host=self.host, port=self.port, db=self.db, **self.dic)
        r = redis.StrictRedis(connection_pool=self.pool)
        return r


def main():
    print('this is redis connection pooling method')
    dic = {'host': '172.16.5.91'}
    redis_pool = RedisWrapper(**dic)
    r = redis_pool.get_cursor()
    res = r.keys('*')
    print('r.keys(*), length is')
    return len(res)

if __name__ == '__main__':
    main()
