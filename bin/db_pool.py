# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
database connection pooling
"""

import redis
import MySQLdb
from read_config import ReadConfig

import sys, time, random



# class Borg(object):
#     _shared_state = {}
#     def __new__(cls, *a, **k):
#         obj  = object.__new__(cls, *a, **k)
#         obj.__dict__ = cls._shared_state
#         return obj

# class RedisWrapperB(Borg):
#     def __init__(self, host='127.0.0.1', port='6379', db='0', name=None):
#         "docstring"
#         self.host=host
#         self.port=int(port)
#         self.db=int(db)
#         if name is not None: self.name = str(name)
#     def redis_connect(self):
#         pool = redis.ConnectionPool(host=self.host, port=self.port, db=self.db)
#         r = redis.StrictRedis(connection_pool=pool)
#         return r

## first want use dict parameter type, intrduce unexpected error,
## next time, use simple thing as possible as you can.

# class RedisWrapper(object):
#     shared_state = {}
#     def __init__(self):
#         self.__dict__ = self.shared_state
#     def redis_connect(self, server_key):
#         redis_server_conf = settings.REDIS_SERVER_CONF['servers'][server_key]
#         connection_pool = redis.ConnectionPool(host=redis_server_conf['HOST'], port=redis_server_conf['PORT'],
#                                                db=redis_server_conf['DATABASE'])
#         return redis.StrictRedis(connection_pool=connection_pool)


# d1 = {'a':'aha',
#       'b':'burn',
#       'c':'extral'}
# def test_dic(a='a', b='b', **dic):
#     print('a: %s, b: %s' % (a, b))
# def test_dic2(c='c', **dic):
#     print(c)
# def test_dic3(c, **dic):
#     print(c)
#     print(dic)
# def test_dic4(a, **dic):
#     print(dic)
# def test_dic5(f, **dic):
#     print(dic)
# def test_dic6(g='g', **dic):
#     print(g)
#     print(dic)

"""
Dictory will be extral parameter, and parsed, pop and give to mathec parameter name.
"""

class MysqlWraperB(object):

    def __init__(self, host='127.0.0.1', port='5432', name='mysql_pool', **dic):
        "docstring"
        self.host = host
        self.port = port
        self.name = name
        # self.cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name=self.name, **dic)
        self.cnxpool = MySQLdb_pool(dic)

    def setconfig(self, new_dic):
        self.cnxpool.set_config(**new_dic)

    def do_work(self, q):
        con = self.cnxpool.get_connection()
        c = c.cursor()
        try:
            c.execute(q)
            res = c.fetchall()
            con.close()
            return res
        except:
            print('query error and raise exception, rooling back.')
            con.rollback()
            con.close()
            return None


class RedisWrapper(object):

    def __init__(self, host='127.0.0.1', port='6379', db='0', name=None):
        "docstring"
        self.host=host
        self.port=int(port)
        self.db=int(db)
        if name is not None: self.name = str(name)

    def redis_connect(self):
        self.pool = redis.ConnectionPool(host=self.host, port=self.port, db=self.db)
        r = redis.StrictRedis(connection_pool=self.pool)
        return r

class MySQLdb_pool(object):
    # create connect instance
    def __init__(self, host='127.0.0.1', db='mysql', user='mysql', maxconnections=5, **conndict):
        self.host = host
        self.db=db
        self.user=user
        self.conndict = conndict
        print(conndict)
        from Queue import Queue
        self._pool = Queue(maxconnections) # create the queue
        self.maxconnections=maxconnections
        # create given amount of connection and add into queue.
        try:
            for i in range(maxconnections):
                self.fillConnection(self.CreateConnection())
        except:
            print('fail init create connection to db')
            raise

    def fillConnection(self,conn):
        try:
            self._pool.put(conn)
        except:
            print(str(sys.exc_info()))

    def returnConnection(self, conn):
        try:
            if  conn and conn.open:
                self._pool.put(conn)
            else:
                print('broken conn, replace at returning back.')
                self.CreateConnection()
        except:
            print(str(sys.exc_info()))

    def getConnection(self):
        try:
            conn = self._pool.get()
            # conn.ping()
            try:
                if not (conn and conn.open):
                    conn = self.CreateConnection()
                    print('pop a broken conn, replace it to valid conn at the out gateway...')
                return conn
            except:
                print('except at out gatway, replacing conn')
                print(str(sys.exc_info()))
                return self.CreateConnection()
        except:
            print(str(sys.exc_info()))

    def ColseConnection(self,conn):
        try:
            # self._pool.get().close()
            conn.close()
            new_conn = self.CreateConnection()
            self.fillConnection(new_conn)
        except:
            print(str(sys.exc_info()))

    def CreateConnection(self):
        try:
            conndb=MySQLdb.connect(host=self.host, db=self.db, user=self.user,  **self.conndict)
            conndb.clientinfo = 'mysql database sync connection pool'
            conndb.ping()
            return conndb
        except:
            print(str(sys.exc_info()))
            print('createConnection error, %s ' % (str(sys.exc_info())))
            raise


class MySQLWrapper(object):

    def __init__(self, host='127.0.0.1', user='mysql', db='5432', **dic):
        self.host = host
        self.db=db
        self.user=user
        self.dic=dic
        print('dic',dic)
        self.pool = MySQLdb_pool(host = self.host, db = self.db, user = self.user, **dic)


    def do_work(self, q):
        con = self.pool.getConnection()
        c = con.cursor()
        try:
            c.execute(q)
            res = c.fetchall()
            self.pool.returnConnection(con)
            return res
        except:
            con.rollback()
            self.pool.returnConnection(con)
            return None


def test_MySQLWrapper(number=100):
    cfg = ReadConfig('./db.info')
    cfgd = cfg.check_config()
    sop = cfgd['mysql']['app_eemsop']
    print('sop', sop)
    ####
    try:
        sql_pool1 = MySQLWrapper(**sop)
    except:
        print('error')
        return -1
    time1_start = time.time()
    for i in range(int(number)):
        sql = 'select * from users where id=%s' % (int(random.random() * 5))
        res = sql_pool1.do_work(sql)
    print(res)
    time1_end = time.time()
    time1_spend = time1_end - time1_start
    print('my own mysql wraper, query  times %s, spend %s s' % (number, time1_spend))
    time2_start = time.time()
    for i in range(int(number)):
        conn = MySQLdb.connect(**sop)
        c = conn.cursor()
        c.execute('select * from users where id=%s' % (int(random.random() * 5)))
        res = c.fetchall()
        conn.close()
    time2_end = time.time()
    time2_spend = time2_end - time1_start
    print('normal, query  times %s, spend %s s' % (number, time2_spend))
    return [time1_spend, time2_spend]

## test passed, 100 query use 5.16s, normal query spend 57.3s

def rd(server='91'):
    cfg = ReadConfig('./db.info')
    cfgd = cfg.check_config()
    try:
        host = '6379' if  not  ('host' in cfgd['redis'][server].keys()) else cfgd['redis'][server]['host']
        port  = '6379' if not ('port' in cfgd['redis'][server].keys()) else cfgd['redis'][server]['port']
        db = '0' if not ('db' in cfgd['redis'][server].keys()) else cfgd['redis'][server]['db']
        # print(host, port, db)
        rp = RedisWrapper(host=host, port=port, db=db, name=server)
    except:
        return None
    return rp.redis_connect()


def main():
    print('db_pool is a database connection pool, as tool class.')
    cfg = ReadConfig('./db.info')
    cfgd = cfg.check_config()
    rp = RedisWrapper(host=cfgd['redis']['91']['host'])
    r1 = rp.redis_connect()
    rp2 = redis.ConnectionPool(host='172.16.5.91', port=6379)
    r2 = redis.Redis(connection_pool=rp2)
    print(r1)
    print(r2)
    test_MySQLWrapper()


if __name__ == '__main__':
    main()
