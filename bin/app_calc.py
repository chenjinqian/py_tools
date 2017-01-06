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
my_pol_n, my_pol_w = mk_mp_d()
rd_pol_c = mk_rp_d()


# def test_func_prop():
#     print(my_pol_d['mysql:app_eemsyd'].pool.cnx_now)
#     con1 = my_pol_d['mysql:app_eemsyd'].pool.getConnection()
#     print(my_pol_d['mysql:app_eemsyd'].pool.cnx_now)
#     return con1


def calc_main(ha=None, hb=None, mid='', app='eemsyd'):
    if not ha or not hb:
        ha, hb = calc_acc(pre=True)
    a, b = calc_acc(ha, hb, mid=mid)
    sql_op(ha, a, hb, b, mid=mid, app=app)
    how_sleep()
    calc_main(a, b, mid=mid, app=app)


def calc_acc(ha=None, hb=None, pre=False, mid='2017'):
    key_lst = mk_tm_key(merter_id, pre=pre)
    a, b = interv(ha, hb)
    return [a, b]


def sql_op(ha, a, hb, b, mid='2017', app='eemsyd'):
    dif_a = ha - a
    dif_b = hb - b
    def mksql(app, kwh=None, pttl=None):
        s = 'select * from %s' % (app)
        reutrn s
    def mksql_last_15_min(mid='2', point=3, table='', time_end='', simple_lst=True):
        import time
        table = '' or 'elec_company_15min_%s' % time.strftime('%Y', time.localtime())
        example_sql = 'select * from (select * from elec_company_15min_2016 where company_id = 2  and stat_time < "2016-12-23 17:45:00" order by stat_time desc, stat_time limit 9600) t order by stat_time;'
        time_end = time_end or fm_tm()
        sql = 'select * from (select * from %s where company_id = %s  and stat_time < "%s" order by stat_time desc, stat_time limit %s) t order by stat_time;' % (table, comp, time_end, point)
        if simple_lst:
            sql = 'select stat_time, kwh from (select * from %s where company_id = %s  and stat_time < "%s" order by stat_time desc, stat_time limit %s) t order by stat_time;' % (table, comp, time_end, point)
        return sql


def how_sleep():
    import time
    sleep_for = 60 * 15
    time.sleep(sleep_for)
    return sleep_for



def main():
    pass

if __name__ == '__main__':
    main()
