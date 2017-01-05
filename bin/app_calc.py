# -*- coding: utf-8 -*-
#!/usr/bin/env python

import read_config as rcfg
import redis_pool as rpol
import mysql_pool as mpol




pool_d = mpol.MySQLWrapper(**cfgd['mysql:app_eemsyd'])
my_pool1 = mpol.MySQLWrapper(**cfgd['mysql:app_eemsyd'])


def mk_my_pol_d(ini='../config/db.ini'):
    cfgd = rcfg.ReadConfig_DB('../config/db.ini').check_config(db_type='mysql', convert_port=True)
    db_lst = [i for i in cfgd.keys() if 'mysql:' in i]
    pol_d = {}
    pol_worker = {}
    for db in db_lst:
        pol_d[db] = mpol.MySQLWrapper(**cfgd[db])
        pol_worker[db] = mpol.MySQLWrapper(**cfgd[db]).do_work
    return [pol_d, pol_worker]

# use dictory for global database connection pool instance.
my_pol_d, my_pol_w = mk_my_pol_d()


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
