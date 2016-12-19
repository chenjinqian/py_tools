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
dbini_d = dbini.check_config()
mysql_app_eemsop = dbini_d['mysql:app_eemsop']


def make_mysql_worker(d = mysql_eemsop):
    pool = mysql_pool.MySQLWrapper(**d)
    return pool

# sop_worker = make_mysql_sop_worker(mysql_eemsop)

sql1 = 'select * from elec_company_15min_2016'
# res1 = sop_worker.do_work(sql1)


from matplotlib import pyplot as plt




def main():
    pass

if __name__ == '__main__':
    main()
