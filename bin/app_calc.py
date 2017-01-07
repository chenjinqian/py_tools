# -*- coding: utf-8 -*-
#!/usr/bin/env python

# Author: ChenJinQian
# Email: 2012chenjinqian@gmail.com


import read_config as rcfg
import redis_pool as rpol
import mysql_pool as mpol


def mk_mp_d(ini='../config/db.ini', mark='mysql:', worker_only=True):
    """read config files, and make mysql connection pool instance as dict."""
    cfgd = rcfg.ReadConfig_DB('../config/db.ini').check_config(db_type='mysql', convert_port=True)
    db_lst = [i for i in cfgd.keys() if mark in i]
    pol_d = {}
    pol_worker = {}
    for db in db_lst:
        pol_d[db] = mpol.MySQLWrapper(**cfgd[db])
        pol_worker[db] = mpol.MySQLWrapper(**cfgd[db]).do_work
    if worker_only:
        return pol_worker
    else:
        return [pol_d, pol_worker]


def mk_rp_d(ini='../config/db.ini', mark='redis:'):
    cfgd = rcfg.ReadConfig_DB('../config/db.ini').check_config(db_type='mysql', convert_port=True)
    db_lst = [i for i in cfgd.keys() if mark in i]
    pol_d = {}
    for db in db_lst:
        pol_d[db] = rpol.RedisWrapper(**cfgd[db]).get_cursor()
    return pol_d


### global variables ###
# use dictory for global database connection pool instance.
cfgd = rcfg.ReadConfig_DB('../config/db.ini').check_config(db_type='mysql', convert_port=True)
mysql_workers_d = mk_mp_d()
redis_cursors_d = mk_rp_d()

from multiprocessing.dummy import Pool as tread_pol
tp4 = tread_pol(4)


def calc_meter_acc(mid=1, time_ckp=0, ha=None):
    # """ha is like {'mid':35545, 'time_ckp': 0, value':112}"""
    v_left, v_near, v_right = get_near_keys(mid, time_ckp)
    a_ttli = kwh_interval(v_near, v='ttli') or ha_ttli
    # TODO: return a list.
    return a


def get_near_keys(mid, ckp_shift, interval=900):
    """redis keys is like r.hget('meterdata_35545_20170106_1600', '20170106_160015')"""
    r = redis_cursors_d['redis:meter']
    def ts_ckp_int(i):
        # TODO: Here, weird int section problem.
        s =  time.strftime('%Y%m%d_%H%M%S', time.localtime(int(time.time())-(int(time.time()) % interval) + i))
        return s
    def time_lst(ckp_shift, left=True, one_key=True):
        import time
        rst = []
        if left:
            ts_k1 = ts_ckp_int(-interval-ckp_shift)[:-2]
            for i in range(interval):
                ts_k2 = ts_ckp_int(- i - ckp_shift)
                rst.append([ts_k1, ts_k2])
                if one_key:
                    break
            return rst
        else:
            ts_k1 = ts_ckp_int(-ckp_shift)[:-2]
            for i in range(interval):
                ts_k2 = ts_ckp_int(i - ckp_shift)
                rst.append([ts_k1, ts_k2])
                if one_key:
                    break
            return rst
    keys_left = time_lst(ckp_shift)
    keys_right = time_lst(ckp_shift, left=False)
    lk1, lk2 = keys_left[0]
    rk1, rk2 = keys_right[0]
    left_keys = sorted(r.hkeys('meterdata_%s_%s' % (mid, lk1)))
    if not left_keys:
        left_key = None
    else:
        left_key = left_keys[-1]
    left_value = r.hget('meterdata_%s_%s' % (mid, lk1), left_key)
    left_values = r.hgetall('meterdata_%s_%s' % (mid, lk1))
    right_keys = sorted(r.hkeys('meterdata_%s_%s' % (mid, rk1)))
    if not right_keys:
        right_key = None
    else:
        right_key = right_keys[0]
    right_value = r.hget('meterdata_%s_%s' % (mid, rk1), right_key)
    right_values = r.hgetall('meterdata_%s_%s' % (mid, rk1))
    # TODO: use keys in redis cursor, get values in four time.
    # chose value nearist in time, but not far than 900s.
    res_d = {}
    if left_key:
        res_d[left_key] = left_value
    if right_key:
        res_d[right_key] = right_value
    else:
        return [left_values, res_d, right_values]


def kwh_interval(d, interval=900, v='ttli'):
    """d is like {time_a:value_a, time_b:value_b}"""
    ks = d.keys()
    rst1 = []
    rst2 = []
    for k in ks:
        val = d[k]
        time_int = int(time.mktime(time.strptime(k, '%Y%m%d_%H%M%S')))
        lst = val.split(',')
        kwhttli_lst = [float(i.split('=')[1]) for i in lst if 'kwhttli=' in i]
        kwhttle_lst = [float(i.split('=')[1]) for i in lst if 'kwhttle=' in i]
        pttli_lst = [float(i.split('=')[1]) for i in lst if 'pttli=' in i]
        if kwhttli_lst:
            rst1 += [time_int, kwhttli_lst[0]]
        if kwhttle_lst:
            rst2 += [time_int, kwhttle_lst[0]]
    def iv(lst):
        x1, y1, x2, y2 = lst
        x0 = x2 - x2 % interval
        rt = y1  + (y2 - y1) * (x0 - x1) / float(x2 - x1)
        return rt
    if rst1:
        kwhttli_0 = iv(rst1)
        if v == 'ttli':
            return kwhttli_0
    if rst2:
        kwhttle_0 = iv(rst2)
        if not v == 'ttli':
            return kwhttle_0
    return None



# qrey_charge(company_id, '2017-01-01 14:30:00', {'company_id':n, 'kwhttli':a, 'pttli':q})


def get_comp_mid(cid, app='mysql:app_eemsyd', comp='company'):
    worker = mysql_workers_d[app]
    sql = 'select related_gmids from %s where id=%s;' % (comp, cid)
    rst = worker(sql)
    if rst:
        mids = rst[0][0].split(',')
    else:
        mids = []
    return [int(i) for i in mids if i]



def how_sleep(shift=180, interval=900, real_sleep=True):
    import time
    sleep_for_i = int(shift) - (int(time.time()) % interval) + interval
    sleep_for = (sleep_for_i + abs(sleep_for_i)) / 2
    print('sleeping for %s' % (sleep_for))
    end_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() + sleep_for))
    print(end_time_str)
    if real_sleep:
        time.sleep(sleep_for)
    return sleep_for


def sql_op(ha, a, hb, b, cmpid='2017', app='eemsop'):
    # TODO: insert one sql into sql.
    dif_a = ha - a
    dif_b = hb - b
    def mksql_15min_insert(stat_time, cmpid, kwh, spfv, charge, table=''):
        import time
        example_sql = "insert into elec_company_15min_2017 (stat_time, cmpid, kwh, charge) values ('2017-01-06 12:00:00' , '7', '100', '80');"
        table = table or 'elec_company_15min_%s' % time.strftime('%Y', time.localtime())
        sql = "insert into %s (stat_time, cmpid, kwh, spfv, charge) \
        values ('%s', '%s', '%s', '%s', '%s');" % (table, stat_time, cmpid, kwh, spfv, charge)
        return sql
    def get_sql_worker(app):
        if not 'mysql:' in str(app):
            app = 'mysql:' + app
        return mysql_workers_d[app]
    def query_charge(cmpid, stat_time):
        pass
    t1 = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    return mksql_15min_insert(t1, cmpid, dif_a, 'f', '3')


d1 = {'a': 0, 'count': 0}

def f1(n, ct= 0, pr=False, *lst):
    # print(d1)
    ct += 1
    if pr:
        print(ct)
    if ct > n:
        return n
    d1['count'] += 1
    import time
    time.sleep(0.05)
    return f1(n - 1, ct)


def f0(lst):
    return f1(*lst)


tp4 = tread_pol(4)
tr4 = tp4.map(f0, [[15,13], [1,2], [22, 4], [22, 1], [4,1], [15, 2]])


def main():
    pass

if __name__ == '__main__':
    main()
