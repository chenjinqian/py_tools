# -*- coding: utf-8 -*-
#!/usr/bin/env python

import read_config as rcfg
import redis_pool as rpol
import mysql_pool as mpol

def help_message():
    h = \
    """
    usage:
    -h: this help message.
    """
    print(h)


def mk_mp_d(ini='../config/db.ini', mark='mysql:'):
    """read config files, and make mysql connection pool instance as dict."""
    cfgd = rcfg.ReadConfig_DB('../config/db.ini').check_config(db_type='mysql', convert_port=True)
    db_lst = [i for i in cfgd.keys() if mark in i]
    pol_d = {}
    pol_worker = {}
    for db in db_lst:
        pol_d[db] = mpol.MySQLWrapper(**cfgd[db])
        pol_worker[db] = mpol.MySQLWrapper(**cfgd[db]).do_work
    return [pol_d, pol_worker]


def mk_rp_d(ini='../config/db.ini', mark='redis:'):
    cfgd = rcfg.ReadConfig_DB('../config/db.ini').check_config(db_type='mysql', convert_port=True)
    db_lst = [i for i in cfgd.keys() if mark in i]
    pol_d = {}
    for db in db_lst:
        pol_d[db] = rpol.RedisWrapper(**cfgd[db]).get_cursor()
    return pol_d


# use dictory for global database connection pool instance.
cfgd = rcfg.ReadConfig_DB('../config/db.ini').check_config(db_type='mysql', convert_port=True)
my_pol_d, my_pol_w = mk_mp_d()
rd_pol_c = mk_rp_d()


def test_func_prop():
    print(my_pool1.pool.cnx_now)
    con1 = my_pool1.pool.getConnection()
    print(my_pool1.pool.cnx_now)
    return con1



def calc_acc(ha=None, hb=None, pre=False, meter_id=''):
    key_lst = mk_tm_key(merter_id, pre=pre)
    a, b = interv(ha, hb)
    return [a, b]


def calc_main(ha=None, hb=None, meter_id=''):
    if not ha or not hb:
        ha, hb = calc_acc(pre=True)
    a, b = calc_acc(ha, hb, meter_id=meter_id)
    sql_op(ha, a, hb, b)
    time.sleep(60 * 15)
    calc_main(a, b, meter_id=meter_id)






def main():
    pass

if __name__ == '__main__':
    main()
