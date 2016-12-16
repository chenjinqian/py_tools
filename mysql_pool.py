# -*- coding: utf-8 -*-
#!/usr/bin/env python

# Author: chenjinqian
# Email: 2012chenjinqina@gmail.com

"""
mysql pool connection wrapper, will auto get and return connection, and rollback.
Example:
    sop = {'db': 'app_eemsop','host': '172.16.9.100','passwd': 'Nf20161201','user': 'h_eems1-refiner'}
    sql_pool1 = MySQLWrapper(**sop)
    sql = 'select * from users where id=%s' % (int(random.random() * 5))
    res = sql_pool1.do_work(sql)
"""

import os, sys, time, random
import MySQLdb

class MySQLdb_pool(object):
    # create connect instance
    def __init__(self, host='127.0.0.1', db='mysql', user='mysql', maxconnections=5, **conndict):
        self.host = host
        self.db=db
        self.user=user
        self.conndict = conndict
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
        # print('dic',dic)
        self.pool = MySQLdb_pool(host = self.host, db = self.db, user = self.user, **dic)


    def do_work(self, q):
        con = self.pool.getConnection()
        c = con.cursor()
        try:
            c.execute(q)
            res = c.fetchall()
            # # should I add this line? will be much longer.
            # c.close()
            self.pool.returnConnection(con)
            return res
        except:
            con.rollback()
            self.pool.returnConnection(con)
            return None


def test_MySQLWrapper(number=13):
    """test passed, in one test, 100 query use 5.16s, normal query spend 57.3s
    [2.5750229358673096,
    2.3464691638946533,
    26.40083909034729,
    29.256152153015137,
    5.96874213218689]
    """
    print('start_testing')
    time0_start = time.time()
    sop = {'db': 'app_eemsop','host': '172.16.9.100','passwd': 'Nf20161201','user': 'h_eems1-refiner'}
    ####
    try:
        sql_pool1 = MySQLWrapper(**sop)
    except:
        print('error')
        return -1
    time1_start = time.time()
    time0_spend = time1_start - time0_start
    for i in range(int(number)):
        sql = 'select * from users where id=%s' % (int(random.random() * 5))
        res = sql_pool1.do_work(sql)
    # print(res)
    time1_end = time.time()
    time1_spend = time1_end - time1_start
    print('my own mysql wraper, query  times %s, spend %s s' % (number, time1_spend))

    time2_start = time.time()
    for i in range(int(number)):
        conn = MySQLdb.connect(**sop)
        c = conn.cursor()
        c.execute('select * from users where id=%s' % (int(random.random() * 5)))
        res = c.fetchall()
        c.close()
        conn.close()
    time2_end = time.time()
    time2_spend = time2_end - time1_start

    time3_start = time.time()
    conn = MySQLdb.connect(**sop)
    c = conn.cursor()
    for i in range(int(number)):
        c.execute('select * from users where id=%s' % (int(random.random() * 5)))
        res = c.fetchall()
    c.close()
    conn.close()
    time3_end = time.time()
    time3_spend = time3_end - time1_start
    print('normal, query  times %s, spend %s s' % (number, time2_spend))

    # # there is no module name DBUtils.PooledDb
    from DBUtils.PooledDB import PooledDB
    pool = PooledDB(creator=MySQLdb, mincached=1 , maxcached=20 ,**sop)
    time4_start = time.time()
    for i in range(int(number)):
        conn = pool.connection()
        c = conn.cursor()
        c.execute('select * from users where id=%s' % (int(random.random() * 5)))
        res = c.fetchall()
        conn.close()
    time4_end = time.time()
    time4_spend = time4_end - time4_start
    return [time0_spend, time1_spend, time2_spend, time3_spend, time4_spend]


def main():
    test_MySQLWrapper(number=100)


if __name__ == '__main__':
    main()
