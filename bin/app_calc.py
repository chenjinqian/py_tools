# -*- coding: utf-8 -*-
#!/usr/bin/env python

# Author: ChenJinQian
# Email: 2012chenjinqian@gmail.com


import read_config as rcfg
# import redis_pool as rpol  # not used
import mysql_pool as mpol
import redis
import time
import itertools
from gevent.pool import Pool as gpol
from gevent import monkey;monkey.patch_all()

# from multiprocessing.dummy import Pool as tpol
# from multiprocessing import Pool as ppol
# tp4 = tpol(4)
# gp4 = gpol(4)
# pp4 = ppol(4)

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
rsrv_default = 'redis:meter'
app_lst_default = ['mysql:app_eemsop']
vrs_s_default = [['kwhttli', 0], ['kwhttle', 0], ['pttl', 2], ['kvarhttli', 0], ['kvarhttle', 0], ['qttl', 2]]
ckps_default = [0, 60*30, 60*60*3]


def sql_get_mids_cids_or_price(cid, option='', app='mysql:app_eemscr', comp='company'):
    worker = mysql_workers_d[app]
    if option == 'price':
        sql = 'select hours, price_p, price_f, price_v  from price_policy  where company_id=%s' % cid
        rst = worker(sql)
        tmp_d = {}
        if rst:
            tmp_d['hours'] = rst[0][0]
            tmp_d['p'] = float(rst[0][1])
            tmp_d['f'] = float(rst[0][2])
            tmp_d['v'] = float(rst[0][3])
        return tmp_d
    elif option == 'company_id':
        sql = 'select id from company;'
        try:
            rst = worker(sql)
        except:
            print('sql query my_except')
            return []
        if rst:
            rst_int = [int(i[0]) for i in rst]
        else:
            rst_int = []
        return rst_int
    elif option == 'workshop_id':
        sql = 'select id from workshop;'
        try:
            rst = worker(sql)
        except:
            print('sql query my_except')
            return []
        if rst:
            rst_int = [int(i[0]) for i in rst]
        else:
            rst_int = []
        return rst_int
    elif option == 'equipment_id':
        sql = 'select id from equipment;'
        try:
            rst = worker(sql)
        except:
            print('sql query my_except')
            return []
        if rst:
            rst_int = [int(i[0]) for i in rst]
        else:
            rst_int = []
        return rst_int
    sql = 'select related_gmids from %s where id=%s;' % (comp, cid)
    rst = worker(sql)
    if rst:
        mids = rst[0][0].split(',')
    else:
        mids = []
    return [str(i) for i in mids if i]


def calc_meter_acc(mid, time_ckp, history=None, vrs_s=vrs_s_default, interval=900, rsrv=rsrv_default, left_null=False):
    # """ha is like {'mid':35545, 'time_ckp': 0, value':112}"""
    # t_start = time.time()
    v_left, v_near, ts_ckp, v_right = get_near_keys(mid, time_ckp, interval=interval, rsrv=rsrv, left_null=left_null)
    # print(mid, time_ckp, time.time() - t_start)
    ckp_values = kwh_interval(v_near, history=history, vrs_s=vrs_s)
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

    def sumup(hst, v_left, ckp, v_right, vr, s='p', factor=3600):
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
        # print(v_sum, v_sum/float(factor))
        return v_sum/float(factor)  # factor for four hours.

    def pstv(y1, y2):
        try:
            y1f = float(y1)
            y2f = float(y2)
        except:
            # print('y1, y2 str, vrs is %s' % (vr))
            return None
        return ((y2f - y1f) + abs(y2f - y1f))/ float(2.0)

    incr = {}
    for hst, ckp, vr in zip(history, ckp_values, vrs_s):
        incr['_times'] = ts_ckp[0]
        if vr[1] == 0:
            # incr.append([vr,ts_ckp[0], pstv(ckp[1], hst[1])])
            incr[vr[0]] = pstv(ckp[1], hst[1])
            # Notice: use leftside of the time section.
        elif vr[1] == 1:
            incr[vr[0]] = sumup(hst, v_left, ckp, v_right, vr[0], 'a')
            # incr.append([vr, ts_ckp[0], sumup(hst, v_left, ckp, v_right, vr[0], 'a')])
        elif vr[1] == 2:
            incr[vr[0]] = [sumup(hst, v_left, ckp, v_right, vr[0], 'p'), sumup(hst, v_left, ckp, v_right, vr[0], 'n')]
            # incr.append([vr, ts_ckp[0], [sumup(hst, v_left, ckp, v_right, vr[0], 'p'), sumup(hst, v_left, ckp, v_right, vr[0], 'n')]])
        else:
            print('type interger not recongnized, should be 0 or 1, get %s' % vr[1])
            # incr.append(None)
    return incr


def get_near_keys(mid, ckp_shift=0, interval=900, rsrv='redis:meter', left_null=False, right_null=True):
    """redis keys is like r.hget('meterdata_35545_20170106_1558', '20170106_160015')"""
    r = redis_cursors_d[rsrv]
    p = r.pipeline(transaction=True)
    # pip = r.pipline(transaction=False)
    def ts_ckp_int(i):
        # TODO: Here, weird int section problem.
        s =  time.strftime('%Y%m%d_%H%M%S', time.localtime(int(time.time())-(int(time.time()) % interval) + int(i)))
        return s
    def time_lst(ckp_shift, left=True, one_key=True):
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
    if not left_null:
        p.hgetall('meterdata_%s_%s' % (mid, lk1))
    else:
        left_values = None
        p.hkeys('meterdata_%s_%s' % (mid, lk1))
    if not right_null:
        p.hgetall('meterdata_%s_%s' % (mid, lk1))
    else:
        right_values = None
        p.hkeys('meterdata_%s_%s' % (mid, rk1))

    # get all result, dict or keys
    [l_dok, r_dok] = p.execute()
    # left_dictory_or_keys
    if not left_null:
        left_values = l_dok
        try:
            left_key = sorted(left_values.keys())[-1] if left_values else None
            # print('left_key %s' % left_key)
        except:
            print(type(left_key), left_key)
        left_value = left_values[left_key] if left_key else None
    else:
        left_keys_raw = l_dok
        left_keys = sorted(left_keys_raw)
        left_key = left_keys[-1] if left_keys else None
        # print('left_key %s' % left_key)
        if left_key:
            p.hget('meterdata_%s_%s' % (mid, lk1), left_key)
            [left_value] = p.execute()
        else:
            left_value = None
    if not right_null:
        right_values = r_dok
        right_key = sorted(right_values.keys())[0] if right_values else None
        right_value = right_values[right_key] if right_key else None
    else:
        right_keys_raw = r_dok
        right_keys = sorted(right_keys_raw)
        right_key = right_keys[0] if right_keys else None
        # print('right_key %s' % right_key)
        if right_key:
            p.hget('meterdata_%s_%s' % (mid, rk1), right_key)
            [right_value] = p.execute()
        else:
            right_value = None
    # left_key = None if not left_keys else left_keys[-1]
    # right_key = None if not right_keys else right_keys[1]
    # p.hget('meterdata_%s_%s' % (mid, lk1), left_key)
    # p.hget('meterdata_%s_%s' % (mid, lk1), right_key)
    # p.hgetall('meterdata_%s_%s' % (mid, lk1)) if not left_null else left_values = None
    # left_keys = sorted(r.hkeys('meterdata_%s_%s' % (mid, lk1)))
    # if not left_keys:
    #     left_key = None
    # else:
    #     left_key = left_keys[-1]
    # left_value = r.hget('meterdata_%s_%s' % (mid, lk1), left_key)
    # if left_null:
    #     left_values = None
    # else:
    #     left_values = r.hgetall('meterdata_%s_%s' % (mid, lk1))
    # right_keys = sorted(r.hkeys('meterdata_%s_%s' % (mid, rk1)))
    # if not right_keys:
    #     right_key = None
    # else:
    #     right_key = right_keys[0]
    # right_value = r.hget('meterdata_%s_%s' % (mid, rk1), right_key)
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


def kwh_interval(d, history=[], vrs_s=vrs_s_default, interval=900):
    """d is like {'20170106_121445':'kwhttli=12,kwhttle=1,pttli=2,pttle=3', '20170106_121509':'kwhttli=18,kwhttle=1.3,pttli=4,pttle=3.2'}"""
    vrs = [i[0] for i in vrs_s]
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


def get_mids_fee_policy(mids, rsrv=rsrv_default, lk1 = 'sync_meterinfo', raw=False, old={}):
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
    pli_d = old
    r = redis_cursors_d[rsrv]
    p = r.pipeline(transaction=False)
    if type(mids) == int:
        mids = [mids]
    for mid in  mids:
        p.hget(lk1, mid)
    fee_plis = p.execute()
    for mid, pli in itertools.izip(mids, fee_plis):
        pli_d[mid] = pli if raw else parse_pli(pli)
    return pli_d


def get_mids_all(rsrv=rsrv_default, lk1 = 'sync_meterinfo', raw=False):
    r = redis_cursors_d[rsrv]
    mids = r.hkeys('sync_meterinfo')
    return mids


def get_all_fee_policy(rsrv=rsrv_default, lk1 = 'sync_meterinfo', raw=False, old={}):
    mids = get_mids_all(rsrv=rsrv, lk1=lk1,raw=raw)
    return get_mids_fee_policy(mids, rsrv=rsrv, lk1=lk1,raw=raw, old=old)


def sql_get_all_info(app_lst=app_lst_default, comp='company'):
    # all comp types.
    mids_pli_d = get_all_fee_policy()
    company_id_d = {} # {'mysql:app_eemscr':[],}
    rst_d = {}
    for app in app_lst:
        import os, sys
        try:
            cid_list = sql_get_mids_cids_or_price(0, option = 'company_id', app=app)
            print(cid_list)
            # workshop_list = sql_get_mids_cids_or_price(0, option = 'workshop_id', app=app)
            # equipment_list = sql_get_mids_cids_or_price(0, option = 'equipment_id', app=app)
            # TODO: get workshop and equipment fees
            rst_d['%s/%s' % (app, comp)] = {}
            # print('%s/%s' % (app, comp))
            for cid in cid_list:
                rst_d['%s/%s' % (app, comp)]['%s' % cid] = {}
        except:
            print('#1',str(sys.exc_info()), rst_d.keys())
    for ap_comp in rst_d.keys():
        for cid in rst_d[ap_comp].keys():
            rst_d[ap_comp][cid] = {}
            meter_id_lst = sql_get_mids_cids_or_price(cid,option='meter_id', app=app)
            rst_d[ap_comp][cid]['meter_id'] = {}
            for mid in meter_id_lst:
                if str(mid) in mids_pli_d:
                    rst_d[ap_comp][cid]['meter_id'][str(mid)] = mids_pli_d[str(mid)]
                else:
                    rst_d[ap_comp][cid]['meter_id'][str(mid)] = {}
    for ap_comp in rst_d.keys():
        for cid in rst_d[ap_comp].keys():
            rst_d[ap_comp][cid]['price'] = sql_get_mids_cids_or_price(cid, option='price', app=app)
    return rst_d


def how_about_sleep(shift=180, interval=900, real_sleep=True):
    sleep_for_i = interval - (int(time.time()) % interval) + int(shift)
    sleep_for = (sleep_for_i + abs(sleep_for_i)) / 2
    print('sleeping for %s' % (sleep_for))
    end_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() + sleep_for))
    print(end_time_str)
    if real_sleep:
        time.sleep(sleep_for)
    return sleep_for


def apply_pli(vr_d, price_d, pli_d, interval=900):
    if not vr_d:
        return {}
    tmp_d = {}
    # print(vr_d, price_d, pli_d)
    index = 3
    tmp_d['spfv'] = price_d['hours'][int(vr_d['_times'][9:11]) - 1]
    rate = price_d[tmp_d['spfv']]
    trans_factor = float(60 * 60 / interval)  # when caculate
    if pli_d['use_energy'] == '0':
        # TODO: Notice, here, change pli_ if data not avaible.
        # default value changed to 1.
        # pttl have two value as list, p+ and p-
        tmp_d['kwhi'] = None if vr_d['pttl'][0] is None else (vr_d['pttl'][0] / trans_factor)
        tmp_d['kwhe'] = None if vr_d['pttl'][1] is None else (vr_d['pttl'][1] / trans_factor)
        tmp_d['kvarhi'] = vr_d['qttl'][0]
        tmp_d['kvarhi'] = vr_d['qttl'][1]
    else:
        tmp_d['kwhi'] = vr_d['kwhttli']
        tmp_d['kwhe'] = vr_d['kwhttle']
        tmp_d['kvarhi'] = vr_d['kvarhttli']
        tmp_d['kvarhe'] = vr_d['kvarhttle']
    if  pli_d['use_power'] == '0':
        tmp_d['p'] = vr_d['kwhttli'] * trans_factor
        tmp_d['q'] = vr_d['kvarhttli'] * trans_factor
    else:
        # TODO: confirm p add up method.
        tmp_d['p'] = ((vr_d['pttl'][0] - vr_d['pttl'][1]) * 4) if not (vr_d['pttl'][0] is None or vr_d['pttl'][1] is None) else None
        # Notice: 60 * 60 / interval
        tmp_d['q'] = ((vr_d['qttl'][0] - vr_d['qttl'][1]) * 4) if not (vr_d['qttl'][0] is None or vr_d['qttl'][1] is None) else None
    if not tmp_d['kwhi'] is None:
        tmp_d['charge'] = rate * tmp_d['kwhi']
    else:
        tmp_d['charge'] = None
    tmp_d['_times'] = vr_d['_times']
    return tmp_d


def key_get_out(ks, n=3):
    ss = ks.split('/')[:n] + ks.split('/')[n+1:]
    s_out = ks.split('/')[n]
    new_s = ''
    for i in ss:
        new_s += i + '/'
    return new_s[:-1], s_out


def one_comp(cid, n=30, mul=True, app='mysql:app_eemscr', comp='company', ckps=ckps_default, interval=900, vrs_s=vrs_s_default, rsrv=rsrv_default, his_d={}, sql_meta_info={}):
    # TODO: pttl unit is hour.
    if not sql_meta_info:
        return {}
    def mk_his_key(mid, t, ckp_ts, no_mid=False):
        # return '%s_%s_%s_%s_%s_%s' % (app, comp, cid, mid, t, ckp_ts)
        if no_mid:
            return '%s/%s/%s/%s' % (app, comp, cid, ckp_ts)
        return '%s/%s/%s/%s/%s' % (app, comp, cid, mid, ckp_ts)

    one_comp_mids = sql_meta_info['%s/%s' % (app, comp)]['%s'%cid]['meter_id'].keys()
    # TODO: use global dict, cache at first 15 mins
    print('cid is %s, mids is %s' % (cid, one_comp_mids))
    flag_key = mk_his_key(0, 0, 0)
    if not flag_key in his_d:
        his_d[flag_key] = 'inited'
        init_history = True
    else:
        init_history = False

    def thread_one(mids, t): # use local varable
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
    def mk_redis_tasks(mids=one_comp_mids, ckps=ckps, cid=cid, interval=interval):
        tasks = []
        if init_history:
            print('init mk_redis_tasks')
            for t in ckps:
                for mid in mids:
                    one_task = [[cid, mid, t+interval, True], [cid, mid, t, False]]
                    tasks += one_task
        else:
            tasks = [[cid, mid, t, False] for mid in mids for t in ckps]
        return tasks

    def rd_acc(lst):
        return rd(*lst)

    def rd(cid, mid, t, left_null):
        rlt_4 = get_near_keys(mid, t, interval=interval, rsrv=rsrv, left_null=left_null)
        # info_str = 'app%s_comp%s_%s_%s_%s_%s' % (app,comp, cid, mid, t, rlt_4[2][1])
        meta_info = [mk_his_key(mid, t, rlt_4[2][0]), mk_his_key(mid, t, rlt_4[2][1])]
        # rlt_4[2] is ts_ckp, will always have values, like ['20170111_120000', '20170111_121500']
        # print('mid', mid)
        return [meta_info, rlt_4]

    def vrs_parse(sect): # use vrs_s outer_side info
        [[key_0, key_1], [v_left, v_near, ts_ckp, v_right]] = sect
        # TODO: try
        # print(key_0, key_1)
        if not key_0 in his_d:
            ckp_values = kwh_interval(v_near, history=[], vrs_s=vrs_s)
            his_d[key_1] = ckp_values
            return []
        else:
            ckp_values = kwh_interval(v_near, history=his_d[key_0], vrs_s=vrs_s)
            his_d[key_1] = ckp_values
            # print(ckp_values)
            incr = incr_sumup(his_d[key_0], v_left, ckp_values, ts_ckp, v_right=v_right, vrs_s=vrs_s)
            return [key_1, incr]

    fee_d_default = {}
    # fees_k is like {'mysql:app_eemscr/company/3/20170111_214500':{}}
    def fee_reduce_mid(vrs_extr_one, fee_d=fee_d_default, sql_meta_info=sql_meta_info):
        def add_up_dic(d1, d2):
            for k1, v1 in d1.items():
                v2 = d2[k1] if k1 in d2 else None
                if type(v1) == str:
                    v_add = v1
                else:
                    v_add = v1 + v2 if (v1 and v2) else (v1 or v2)
                d1[k1] = v_add
            return d1

        # TODO: should merge fee_d and pli_d into one dict.
        ks, vr_d = vrs_extr_one
        # # vrs_extr_one is like[meter_info_str, dict]
        # # ks is like 'mysql:app_eemscr/company/3/20170101_121500'
        cid_fee_key, meter_id = key_get_out(ks)
        cid = key_get_out(ks, 2)[1]
        price_info_d = sql_meta_info['%s/%s' % (app, comp)]['%s'%cid]['price']
        pli_info_d   = sql_meta_info['%s/%s' % (app, comp)]['%s'%cid]['meter_id'][meter_id]
        # print(vr_d, price_info_d, pli_info_d)
        # time.sleep(1000)
        # print(vr_d)
        fee_meter_new = apply_pli(vr_d, price_info_d, pli_info_d)
        if not cid_fee_key in fee_d:
            fee_d[cid_fee_key] = fee_meter_new
        else:
            fee_meter_exist_one = fee_d[cid_fee_key]
            fee_d[cid_fee_key] = add_up_dic(fee_meter_exist_one, fee_meter_new)
        return fee_meter_new

    t_s = time.time()
    # print(len(mk_redis_tasks(mids, ckps)))
    tp = gpol(n)
    # redis_rcds = tp.map(t_acc, [[mid, i] for mid in mids for i in ckps])
    redis_rcds = tp.map(rd_acc, mk_redis_tasks())
    # t_b = time.time()
    # TODO: pipe the data process, which will save RAM.
    vrs_extr = [i for i in map(vrs_parse, redis_rcds) if i]
    fee_lst = map(fee_reduce_mid, vrs_extr)
    # print('cpu time %s' % (time.time() - t_b))
    print('spend time %s' % (time.time() - t_s))
    # return vrs_extr
    # return [redis_rcds, vrs_extr, fee_lst, fee_d_default]
    print( len(redis_rcds), len(vrs_extr), len(fee_lst))
    # return [fee_lst, fee_d_default]
    return fee_d_default


sql_meta_info_default = sql_get_all_info(app_lst_default)
# sql_meta_info_default = {}
his_d_default = {}

def snip_shot(meta_d=sql_meta_info_default, his_d=his_d_default):
    app_comps = meta_d.keys()
    def produce_task(meta_d=meta_d):
        for app_comp in app_comps:
            app, comp = app_comp.split('/')
            print(app)
            cids = meta_d[app_comp].keys()
            for cid in cids:
                print(cid)
                # yield [int(cid), app]
                yield (int(cid), app)
    # rst_snp = [one_comp(i[0],n=20, app=i[1]) for i in produce_task()]
    p90 = gpol(90)
    rst_snp = p90.map(lambda lst: one_comp(lst[0], app=lst[1], his_d=his_d, sql_meta_info=sql_meta_info), (i for i in produce_task()))
    return rst_snp


# TODO: kvarhi, kvarhe and q
    # 'mysql:app_eemsii',
    # 'mysql:app_eemsdemo',
    # 'mysql:meterinfo',
    # 'mysql:app_eemsakuu',
    # 'mysql:app_eemssec',
    # 'mysql:app_eemscr',
    # 'mysql:app_eemsman',
    # 'mysql:app_eemsyd',
# TODO: 15 min init and end of loop, inti values

# TODO: make it a class
# t_start = time.time()
# rst_snipshot2 = snip_shot()
# print('total spend time %s' % (time.time() - t_start))
# rst_snipshot2_valid = [i for i in rst_snipshot2 if i]


def test_redis(fn=get_near_keys, lst = [2, 0 ], args={}):
    t_start = time.time()
    # near_keys = get_near_keys(2, 0)
    near_keys = fn(*lst, **args)
    print(time.time() - t_start)
    return near_keys


# testing data:
# elec_ table is like
# (('stat_time', 'datetime', 'NO', 'PRI', None, ''),
#  ('company_id', 'smallint(5) unsigned', 'NO', 'PRI', None, ''),
#  ('kwh', 'double', 'NO', '', None, ''),
#  ('spfv', 'char(1)', 'NO', '', None, ''),
#  ('charge', 'double', 'NO', '', None, ''),
#  ('p', 'double', 'YES', '', None, ''),
#  ('kwhi', 'double', 'YES', '', None, ''),
#  ('kwhe', 'double', 'YES', '', None, ''),
#  ('q', 'double', 'YES', '', None, ''),
#  ('kvarhi', 'double', 'YES', '', None, ''),
#  ('kvarhe', 'double', 'YES', '', None, ''))

vr_ext_one = ['mysql:app_eemscr/company/3/2166/20170111_194500',
              {'kwhttle': None,
               'kwhttli': None,
               'pttl': [194.30324470, 0.0],
               '_times': '20170111_193000'}]

comp_info_d_one = {'meter_ids': [34758, 2159, 2166, 2024, 2016, 2019, 2125, 2000, 2075, 2106],
                   'price': {'f': 0.5846,
                             'hours': 'vvvvvvvvfpppfffffffpppff',
                             'p': 0.9329,
                             'v': 0.3167}}

pli_one = {'ctnum': '2',
             'ctr': '10.0',
             'dev_mac': '120000af6b3a',
             'dev_model': 'pb600',
             'gmid': '35545',
             'kva': '800.0',
             'meter_sn': '',
             'ptr': '100.0',
             'r_ia': '10.0',
             'r_kwhttli': '1000.0',
             'r_pa': '1000.0',
             'r_ua': '100.0',
             'tc': '800.0',
             'use_energy': '0',
             'use_power': '0'}

fee_d_one = {'charge': 113.58967685162,
             'kwhe': 0.0,
             'kwhi': 194.3032447,
             'p': 777.2129788,
             'spfv': 'f',
             '_times': '20170111_193000'}


def main():
    pass

if __name__ == '__main__':
    main()
