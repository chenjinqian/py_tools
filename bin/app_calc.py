# -*- coding: utf-8 -*-
#!/usr/bin/env python

# Author: ChenJinQian
# Email: 2012chenjinqian@gmail.com


import read_config as rcfg
# import redis_pool as rpol  # not used
import mysql_pool as mpol
import redis
import time


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
        # pol_d[db] = rpol.RedisWrapper(**cfgd[db]).get_cursor
        p = redis.ConnectionPool(**cfgd[db])
        r = redis.Redis(connection_pool=p)
        pol_d[db] = r
    return pol_d


### global variables ###
# use dictory for global database connection pool instance.

cfgd = rcfg.ReadConfig_DB('../config/db.ini').check_config(db_type='mysql', convert_port=True)
mysql_workers_d = mk_mp_d()
redis_cursors_d = mk_rp_d()


# from multiprocessing.dummy import Pool as tpol
# tp4 = tpol(4)
from gevent.pool import Pool as gpol
from gevent import monkey
monkey.patch_all()

# from gevent import monkey
# gp4 = gpol(4)
# from multiprocessing import Pool as ppol
# pp4 = ppol(4)

vrs_s_default = [['kwhttli', 0], ['kwhttle', 0], ['pttl', 2]]
ckps_default = [0, 60*30, 60*60*3]
rsrv_default = 'redis:meter'

def test1(n=2, d={}):
    a= 0
    print(a, d)
    if not a:
        a = a + 2
    if not d:
        d['a'] = a
    time.sleep(0.3)
    if n < 0:
        return a
    return test1((n - 1), {})


def one_comp(cid, n, mul=True, app='mysql:app_eemsyd', comp='company', ckps=ckps_default, interval=900, vrs_s=vrs_s_default, rsrv=rsrv_default):
    # TODO: pttl unit is hour.
    import time
    his_d = {}
    ic_sum = []
    mids = get_comp_mid(cid, app=app, comp=comp)
    # tp = tread_pol(len(mids) * len(ckps))
    # tp = tread_pol(12)
    # tp = gpol(12)

    def thread_one(mids, t):
        if  not str(str(t) +'_'+ str(mid)) in his_d:
            ic0, hi0 = calc_meter_acc(mid, t + interval, vrs_s=vrs_s)
            # print(hi0)
            his_d[str(t) +'_'+ str(mid)] = hi0
        ic, hi = calc_meter_acc(mid, t, history=his_d[str(t) + '_' + str(mid)], vrs_s=vrs_s)
        # print(ic, hi)
        his_d[str(t) +'_'+ str(mid)] = hi
        return ic

    def t_acc(lst):
        return thread_one(*lst)

    def mk_redis_tasks(mids=mids, ckps=ckps, cid=cid, interval=interval):
        tasks = []
        for t in ckps:
            for mid in mids:
                one_task = [[cid, mid, t, True], [cid, mid, t + interval, False]]
                tasks += one_task
        return tasks

    def rd_acc(lst):
        return rd(*lst)

    def rd(cid, mid, t, left_null):
        rlt_4 = get_near_keys(mid, t, interval=interval, rsrv=rsrv, left_null=left_null)
        # rlt_4 = [1,2,[1,2],4]
        # time.sleep(0.1)
        d= {}
        key = '%s_%s_%s_%s' % (cid, mid, t, rlt_4[2][1])
        # rlt_4[2] is ts_ckp, will always have values, like ['20170111_120000', '20170111_121500']
        d[key] = rlt_4
        return rlt_4

    t_s = time.time()
    print(len(mk_redis_tasks(mids, ckps)))
    if mul:
        tp = gpol(n)
        # ic_sum = tp.map(t_acc, [[mid, i] for mid in mids for i in ckps])
        ic_sum = tp.map(rd_acc, mk_redis_tasks())
    else:
        ic_sum = []
    print(time.time() - t_s)
    # print(his_d.keys())
    return ic_sum

# ta = time.time()
# comp2 = one_comp(2)
# print(time.time() - ta)

def calc_meter_acc(mid, time_ckp, history=None, vrs_s=vrs_s_default, interval=900, rsrv=rsrv_default, left_null=False):
    # """ha is like {'mid':35545, 'time_ckp': 0, value':112}"""
    vrs= [i[0] for i in vrs_s]
    # t_start = time.time()
    v_left, v_near, ts_ckp, v_right = get_near_keys(mid, time_ckp, interval=interval, rsrv=rsrv, left_null=left_null)
    # print(mid, time_ckp, time.time() - t_start)
    ckp_values = kwh_interval(v_near, vrs=vrs, history=history)
    incr = incr_sumup(history, v_left, ckp_values, ts_ckp, v_right=v_right, vrs_s=vrs_s)
    return [incr, ckp_values]


def incr_sumup(history, v_left, ckp_values, ts_ckp, v_right=None, vrs_s=[['kwhttli', 0]]):
    """history is """
    if not history or not v_left or not ckp_values:
        return None
    def vr_parse2(sss, vr):
        """sss is like 'var1=2,var3=2', ss is like ['var1=num1', 'var2=num2']"""
        ss = sss.split(',')
        try:
            numbers = [float(i.split('=')[1]) for i in ss if str(vr)+'=' in i]
            num = None if not numbers else numbers[0]
            return num
        except:
            return None
    def int_f_k(k):
        return int(time.mktime(time.strptime(k, '%Y%m%d_%H%M%S')))

    def sumup(hst, v_left, ckp, v_right, vr, s='p'):
        """s = p/n/a, for positive, negative, as is."""
        if not v_left or not type(v_left)==dict:
            return None
        try:
            hst_one = float(hst[1])
            ckp_one = float(ckp[1])
        except:
            return None
        left_keys = sorted(v_left.keys())
        # right_keys = sorted(v_right.keys())
        # int_v_right = [[int_f_k(k), float(v_right[k])] for k in right_keys]
        int_v_left = [[int_f_k(k), vr_parse2(v_left[k], vr)] for k in left_keys]
        int_hst = [[int_f_k(ts_ckp[0]), hst_one]]
        int_ckp = [[int_f_k(ts_ckp[1]), ckp_one]]
        # # use new checkpoint time_string, cause history can be inhere
        ti_value_all = int_hst + int_v_left + int_ckp
        v_sum = float(0)
        def po(n):
            return (abs(n) + n) / float(2)
        def ne(n):
            return (n - abs(n)) / float(2)
        for i in zip(ti_value_all[:-1], ti_value_all[1:]):
            [x1, y1], [x2, y2] = i
            if  s=='n':
                acc = 0.0 if (y1 is None or y2 is None) else ne(y1 + y2)*(x2 - x1) / float(2)
            elif s == 'p':
                acc = 0.0 if (y1 is None or y2 is None) else po(y1 + y2)*(x2 - x1) / float(2)
            else:
                acc = 0.0 if (y1 is None or y2 is None) else (y1 + y2)*(x2 - x1) / float(2)
            # print(acc, x1, y1, x2, y2)
            v_sum += acc
        return v_sum

    def pstv(y1, y2):
        try:
            y1f = float(y1)
            y2f = float(y2)
        except:
            # print('y1, y2 str, vrs is %s' % (vr))
            return None
        return ((y2f - y1f) + abs(y2f - y1f))/ float(2.0)

    incr = []
    for hst, ckp, vr in zip(history, ckp_values, vrs_s):
        if vr[1] == 0:
            incr.append([ts_ckp[0], pstv(ckp[1], hst[1])])
            # Notice: use leftside of the time section.
        elif vr[1] == 1:
            incr.append([ts_ckp[0], sumup(hst, v_left, ckp, v_right, vr[0], 'a')])
        elif vr[1] == 2:
            incr.append([ts_ckp[0], [sumup(hst, v_left, ckp, v_right, vr[0], 'p'), sumup(hst, v_left, ckp, v_right, vr[0], 'n')]])
        else:
            print('type interger not recongnized, should be 0 or 1, get %s' % vr[1])
            incr.append(None)
    return incr

# # testing
# # one redis recd for meter 35545, is like:
# history1 = [['20170106_121500', 20.2],
#             ['20170106_121500', 1.2],
#             ['20170106_121500', 2.2],
#             ['20170106_121500', 11.2]]
# ckp_values1 = [['20170106_121500', 41.1],
#                ['20170106_121500', 3.1],
#                ['20170106_121500', 3.1],
#                ['20170106_121500', 14.1]]
# v_left1 = {'20170106_120315': 'kwhttli=42,kwhttle=3,pttli=2,pttle=3',
#           '20170106_120625': 'kwhttli=31,kwhttle=2,pttli=2,pttle=3',
#           '20170106_121135': 'kwhttli=21,kwhttle=1,pttli=2,pttle=3',
#           '20170106_121445': 'kwhttli=32,kwhttle=2,pttli=2,pttle=3'}
# ts_ckp1 =['20170106_120000', '20170106_121500']
# vrs_s1 = [['kwhttli', 0],
#           ['kwhttle', 0],
#           ['pttli', 1],
#           ['pttle', 1]]
#incr1 =  incr_sumup(history1, v_left1, ckp_values1, ts_ckp1, None, vrs_s1)

# t_a = time.time()
# pa= get_mids_fee_policy(mids_all)
# # mids_all = get_mids_all()
# # poli_all = get_all_fee_policy()
# print(time.time() - t_a)


def get_near_keys(mid, ckp_shift, interval=900, rsrv='redis:meter', left_null=False, right_null=True):
    """redis keys is like r.hget('meterdata_35545_20170106_1558', '20170106_160015')"""
    r = redis_cursors_d[rsrv]
    p = r.pipeline(transaction=False)
    # TODO: use piplining and transaction.
    # pip = r.pipline(transaction=False)
    def ts_ckp_int(i):
        # TODO: Here, weird int section problem.
        s =  time.strftime('%Y%m%d_%H%M%S', time.localtime(int(time.time())-(int(time.time()) % interval) + int(i)))
        return s
    def time_lst(ckp_shift, left=True, one_key=True):
        import time
        rst = []
        if left:
            ts_k1 = ts_ckp_int(-interval-ckp_shift)[:-2]
            if one_key:
                return [[ts_k1, None]]
            for i in range(interval):
                ts_k2 = ts_ckp_int(- i - ckp_shift)
                rst.append([ts_k1, ts_k2])
            return rst
        else:
            ts_k1 = ts_ckp_int(-ckp_shift)[:-2]
            if one_key:
                return [[ts_k1, None]]
            for i in range(interval):
                ts_k2 = ts_ckp_int(i - ckp_shift)
                rst.append([ts_k1, ts_k2])
            return rst
    keys_left = time_lst(ckp_shift)
    keys_right = time_lst(ckp_shift, left=False)
    lk1, lk2 = keys_left[0]
    rk1, rk2 = keys_right[0]
    ### added
    p.hkeys('meterdata_%s_%s' % (mid, lk1))
    p.hkeys('meterdata_%s_%s' % (mid, rk1))
    left_keys_raw, right_keys_raw = p.execute()
    left_keys, right_keys = sorted(left_keys_raw), sorted(right_keys_raw)
    left_key = None if not left_keys else left_keys[-1]
    right_key = None if not right_keys else right_keys[1]
    p.hget('meterdata_%s_%s' % (mid, lk1), left_key)
    p.hget('meterdata_%s_%s' % (mid, lk1), right_key)
    p.hgetall('meterdata_%s_%s' % (mid, lk1)) if not left_null else left_values = None

    ### end
    # left_keys = sorted(r.hkeys('meterdata_%s_%s' % (mid, lk1)))
    # if not left_keys:
    #     left_key = None
    # else:
    #     left_key = left_keys[-1]
    # left_value = r.hget('meterdata_%s_%s' % (mid, lk1), left_key)
    if left_null:
        left_values = None
    else:
        left_values = r.hgetall('meterdata_%s_%s' % (mid, lk1))
    right_keys = sorted(r.hkeys('meterdata_%s_%s' % (mid, rk1)))
    if not right_keys:
        right_key = None
    else:
        right_key = right_keys[0]
    right_value = r.hget('meterdata_%s_%s' % (mid, rk1), right_key)
    if right_null:
        right_values = None
    else:
        right_values = r.hgetall('meterdata_%s_%s' % (mid, rk1))
    # TODO: right keys is not used, could return none.
    # TODO: use keys in redis cursor, get values in four time.
    # chose value nearist in time, but not far than 900s.
    res_d = {}
    if left_key:
        res_d[left_key] = left_value
    if right_key:
        res_d[right_key] = right_value
    ts_ckp = [ts_ckp_int(-interval-ckp_shift), ts_ckp_int(-ckp_shift)]
    return [left_values, res_d, ts_ckp, right_values]


def kwh_interval(d, history=[], vrs=['kwhttli','pttli', 'pttle'], interval=900):
    """d is like {'20170106_121445':'kwhttli=12,kwhttle=1,pttli=2,pttle=3', '20170106_121509':'kwhttli=18,kwhttle=1.3,pttli=4,pttle=3.2'}"""
    def vr_parse(ss, var_str):
        """ss is like ['var1=num1', 'var2=num2']"""
        try:
            numbers = [float(i.split('=')[1]) for i in ss if str(var_str)+'=' in i]
            num = None if not numbers else numbers[0]
            return num
        except:
            return None

    def iv(lst):
        x1, y1, x2, y2 = lst
        x0 = x2 - x2 % interval
        if (x1 is None or x2 is None or y1 is None or y2 is None):
            # print(x1, y1, x2, y2)
            # print('variable none.')
            return [x0, None]
        if float(x2 - x1) == 0:
            rt = [x1, float(y1 + y2)/2]
        else:
            rt = [x0, y1  + (y2 - y1) * (x0 - x1) / float(x2 - x1)]
        return rt

    import time
    # print('vrs is:%s' % (vrs))
    if not d:
        return history
    ks = sorted(d.keys())
    vrs_all = []
    # rst1 = [] # kwhttli
    # rst2 = [] # kwhttle
    for k in ks:
        val = d[k]
        time_int = int(time.mktime(time.strptime(k, '%Y%m%d_%H%M%S')))
        lst = val.split(',')
        vrs_num_acc = [[time_int, vr_parse(lst, i)] for i in vrs]
        vrs_all.append(vrs_num_acc)
    # vrs_all is like [[[1221212, 3.2], [12323213, 4.1],...], [[], [], ...]]
    if len(vrs_all) == 1:
        vrs_1 = vrs_2 = vrs_all[0]
    else:
        vrs_1, vrs_2 = vrs_all
    # TODO: if only one side have values.s
    rst = []
    for a, b in zip(vrs_1, vrs_2):
        x0, y0 = iv(a + b)
        rst.append([time.strftime('%Y%m%d_%H%M%S', time.localtime(x0)), y0])
    return rst


def get_comp_mid(cid, app='mysql:app_eemsyd', comp='company'):
    worker = mysql_workers_d[app]
    sql = 'select related_gmids from %s where id=%s;' % (comp, cid)
    rst = worker(sql)
    if rst:
        mids = rst[0][0].split(',')
    else:
        mids = []
    return [int(i) for i in mids if i]

# mids_all = get_mids_all()
# gp8  = gpol(1000)
# ta  = time.time()
# a13 = gp8.map(get_mids_fee_policy, mids_all)
# 5s, with 64 gevent pool, 8s with 1000 gpol.
# pli_all = get_all_fee_policy()
# 2s
# print(time.time() - ta)

def get_all_fee_policy(rsrv=rsrv_default, lk1 = 'sync_meterinfo', raw=False):
    mids = get_mids_all(rsrv=rsrv, lk1=lk1,raw=raw)
    return get_mids_fee_policy(mids, rsrv=rsrv, lk1=lk1,raw=raw)


def get_mids_all(rsrv=rsrv_default, lk1 = 'sync_meterinfo', raw=False):
    r = redis_cursors_d[rsrv]
    mids = r.hkeys('sync_meterinfo')
    return mids


def get_mids_fee_policy(mids, rsrv=rsrv_default, lk1 = 'sync_meterinfo', raw=False):
    def parse_pli(sss):
        d = {}
        if not sss:
            return d
        ss = [s for s in sss.split(',') if s]
        for s in ss:
            try:
                k, v = s.split('=')
                d[k] = v
            except:
                # print(s)
                continue
        return d
    pli_d = {}
    r = redis_cursors_d[rsrv]
    p = r.pipeline(transaction=False)
    if type(mids) == int:
        mids = [mids]
    for mid in  mids:
        p.hget(lk1, mid)
    fee_plis = p.execute()
    for mid, pli in zip(mids, fee_plis):
        pli_d[mid] = pli if raw else parse_pli(pli)
    return pli_d


def how_about_sleep(shift=180, interval=900, real_sleep=True):
    import time
    sleep_for_i = interval - (int(time.time()) % interval) + int(shift)
    sleep_for = (sleep_for_i + abs(sleep_for_i)) / 2
    print('sleeping for %s' % (sleep_for))
    end_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() + sleep_for))
    print(end_time_str)
    if real_sleep:
        time.sleep(sleep_for)
    return sleep_for


def sql_op(ha, a, hb, b, cmpid='2017', app='eemsyd', comp='company'):
    # TODO: insert one sql into sql.
    # invalide.
    dif_a = ha - a
    dif_b = hb - b
    def mksql_15min_insert(stat_time, cmpid, kwh, spfv, charge, table=''):
        import time
        example_sql = "insert into elec_company_15min_2017 (stat_time, cmpid, kwh, charge) values ('2017-01-06 12:00:00' , '7', '100', '80');"
        table = table or 'elec_%s_15min_%s' % (comp, time.strftime('%Y', time.localtime()))
        sql = "insert into %s (stat_time, cmpid, kwh, spfv, charge)  values ('%s', '%s', '%s', '%s', '%s');" % (table, stat_time, cmpid, kwh, spfv, charge)
        return sql

    def get_sql_worker(app):
        if not 'mysql:' in str(app):
            app = 'mysql:' + app
        return mysql_workers_d[app]
    def query_charge(cmpid, stat_time):
        return None
    t1 = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    return mksql_15min_insert(t1, cmpid, dif_a, 'f', '3')


def f1(n, ct= 0, pr=False, *lst):
    # print(d1)
    ct += 1
    if pr:
        print(ct)
    if ct > n:
        return n
    # d1['count'] += 1
    import time
    time.sleep(0.08)
    return f1(n - 1, ct)

def f0(lst):
    return f1(*lst)


def fln(fn):
    def f_acc(lst):
        return fn(*lst)
    return f_acc


def test_thread():
    f_02 = fln(f1)
    pp4 = ppol(4)
    tp4 = tpol(4)
    trst4 = tp4.map(f_02, [[15,13], [1,2], [22, 4], [22, 1], [4,1], [15, 2]])
    # prst4 = pp4.map(f_02, [[15,13], [1,2], [22, 4], [22, 1], [4,1], [15, 2]])
    # can not use dynamic
    print(trn4)
    nn4 = pp4.map(f1, [5,3,8,7])
    print(nn4)
    # gp8 = gpol(8)
    # gp4 = gpol(4)
    # gp2 = gpol(2)
    # ta = time.time()
    # gr4 = gp4.map(fln(f1), [[21,3], [21,2], [22, 4], [22, 1], [24,1], [25, 2]])
    # print(time.time() - ta)

    # trl4 = tp4.map(fln, [[15,13], [1,2], [22, 4], [22, 1], [4,1], [15, 2]])
    # tr4 = tp4.map(f0, [[15,13], [1,2], [22, 4], [22, 1], [4,1], [15, 2]])

    # qrey_charge(company_id, '2017-01-01 14:30:00', {'company_id':n, 'kwhttli':a, 'pttli':q})


def test_redis(fn=get_near_keys, lst = [2, 0 ], args={}):
    import time
    t_start = time.time()
    # near_keys = get_near_keys(2, 0)
    near_keys = fn(*lst, **args)
    print(time.time() - t_start)
    return near_keys


def main():
    pass

if __name__ == '__main__':
    main()
