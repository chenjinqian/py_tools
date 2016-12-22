# -*- coding: utf-8 -*-
#!/usr/bin/env python

import read_config
import redis_pool
import mysql_pool


# mysql_pool.test_MySQLWrapper()
# dic = {'host': '172.16.5.91'}
# redis_pool = RedisWrapper(**dic)
# r = redis_pool.get_cursor()

cfg = read_config.ReadConfig('../config/db.info')
cfgd = cfg.check_config()
mysql_eemsop = cfgd['mysql']['app_eemsop']

dbini = read_config.ReadConfig_DB('../config/db.ini')
dbini_d = dbini.check_config(db_type='mysql', convert_port=True)


# def make_mysql_worker(d = dbini_d['mysql:app_eemsop']):
#     pool = mysql_pool.MySQLWrapper(**d)
#     return pool
# sop_worker = make_mysql_worker(dbini_d['mysql:app_eemsop'])


# sop_pool = mysql_pool.MySQLWrapper(**dbini_d['mysql:app_eemsop'])
# sopw = sop_pool.do_work
# sec_pool = mysql_pool.MySQLWrapper(**dbini_d['mysql:app_eemssec'])
# secw = sec_pool.do_work
# syd_pool = mysql_pool.MySQLWrapper(**dbini_d['mysql:app_eemsyd'])
# sydw = syd_pool.do_work
mdt_pool = mysql_pool.MySQLWrapper(**dbini_d['mysql:meterdataxbxb'])

tables = sop_pool.do_work('show tables;')
for tb in tables:
    print('select count(*) from %s' % (tb))
    print(sop_pool.do_work('select count(*) from %s' % (tb)))


tables = secw('show tables;')
for tb in tables:
    print('select count(*) from %s' % (tb))
    print(secw('select count(*) from %s' % (tb)))


def get_15m_last_n(company_id, n=2, table_name='elec_company_15min_2016', order_by='stat_time', dic=mysql_app_eemsop):
    worker = mysql_pool.MySQLWrapper(**dic)
    sql_example = 'select * from elec_company_15min_2016 where company_id = 12 order by stat_time desc limit 5;'
    sql = 'select * from %s where company_id=%s order by %s desc limit %s;' % (table_name, company_id, order_by, n)
    res = worker.do_work(sql)
    return res


# sydw('select stat_time, company_id, kwh  from elec_company_15min_2016 order by stat_time DESC,  company_id limit 50;')

from matplotlib import pyplot as plt


def wnd(lst, n=1, both_side=True):
    n_lst = len(lst)
    n_wd = n
    res = []
    for i in range(n_lst):
        if not both_side:
            s = range(i-n, i+n) # not include right side
        else:
            s = range(i-n, i+n+1)
        s1 = [x for x in s if x>0]
        s2 = [x for x in s1 if x<n_lst]
        avar = sum([lst[i] for i in s2]) / float(len(s2))
        res.append(avar)
    return res


def wns(lst, fold, n=1, both_side=True):
    for i in range(int(fold)):
        lst = wnd(lst, n=n, both_side=both_side)
    return lst



def plot_one_company_line(company_id, n=96):
    res = get_15m_last_n(company_id, n=n)
    elcs = [i[2] for i in res]
    fees = [i[4] for i in res]
    xs = [x for x in range(n)]
    plt.plot(xs, wns(elcs, 3))
    plt.plot(xs, wns(fees))
    plt.show()
    return len[elcs, fees]


def plot_test():
    xs = [i for i in range(100)]
    ys = [(i**2 - 5*i) for i in xs]
    print(xs, ys)
    plt.plot((xs, ys))


# sql1 = 'select * from elec_company_15min_2016'
# res1 = sop_worker.do_work(sql1)






def main():
    pass

if __name__ == '__main__':
    main()
