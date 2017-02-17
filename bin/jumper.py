# -*- coding: utf-8 -*-
#!/usr/bin/env python

import read_config as rcfg
# import redis_pool as rpol
import redis
import mysql_pool as mpol

from matplotlib import pyplot as plt
import numpy as np
import scipy as sp
import time

import time, os, sys

# dbini = read_config.ReadConfig_DB('../config/db.ini')
# dbini_d = dbini.check_config(db_type='mysql', convert_port=True)
# r_webpage_pool = redis_pool.RedisWrapper(**dbini_d['redis:webpage'])
# r = r_webpage_pool.get_cursor()
# sop_pool = mysql_pool.MySQLWrapper(**dbini_d['mysql:app_eemsop'])
# sopw = sop_pool.do_work
# sec_pool = mysql_pool.MySQLWrapper(**dbini_d['mysql:app_eemssec'])
# secw = sec_pool.do_work
# syd_pool = mysql_pool.MySQLWrapper(cnx_num=2,**dbini_d['mysql:app_eemsyd'])
# sydw = syd_pool.do_work
# scr_pool = mysql_pool.MySQLWrapper(**dbini_d['mysql:app_eemscr'])
# scrw = syd_pool.do_work

def mk_mp_d(ini='../config/db.ini', mark='mysql:'):
    """read config files, and make mysql connection pool instance as dict."""
    cfgd = rcfg.ReadConfig_DB('../config/db.ini').check_config(db_type='mysql', convert_port=True)
    db_lst = [i for i in cfgd.keys() if mark in i]
    # pol_d = {}
    pol_worker = {}
    for db in db_lst:
        # pol_d[db] = mpol.MySQLWrapper(**cfgd[db])
        pol_worker[db] = mpol.MySQLWrapper(**cfgd[db]).do_work
    return pol_worker


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


# use dictory for global database connection pool instance.
cfgd = rcfg.ReadConfig_DB('../config/db.ini').check_config(db_type='mysql', convert_port=True)
mysql_worker_d = mk_mp_d()
redis_cursor_d = mk_rp_d()


# sydw('select stat_time, company_id, kwh from elec_company_15min_2016 order by stat_time DESC, company_id limit 50;')
# mdt_pool = mysql_pool.MySQLWrapper(**dbini_d['mysql:meterdataxbxb'])


def fm_tm(style=0, fm = '', tm=''):
    import time
    """3 style of time."""
    if style == 4:
        fm = '%Y-%m-%d 00:00:00'
    if style == 2:
        fm = '%Y%m%d_%H%M%S'
    elif style == 1:
        fm =  '%Y%m%d%H%M%S'
    elif style == 3:
        fm = '%Y-%m-%d_%H:%M:%S'
    else:
        fm = fm or '%Y-%m-%d %H:%M:%S'
    tm = tm or time.localtime()
    return time.strftime(fm, tm)


def mksql_last_15_min(company_id='2', point=3, time_end='', simple_lst=True, use_kwhi=True, comp='company'):
    table = '' or 'elec_%s_15min_%s' % (comp, time.strftime('%Y', time.localtime()))
    example_sql = 'select * from (select * from elec_company_15min_2016 where company_id = 2  and stat_time < "2016-12-23 17:45:00" order by stat_time desc, stat_time limit 9600) t order by stat_time;'
    time_end = time_end or fm_tm()
    sql = 'select * from (select * from %s where %s_id = %s  and stat_time < "%s" order by stat_time desc, stat_time limit %s) t order by stat_time;' % ( table, comp, company_id, time_end, point)
    if simple_lst:
        sql = 'select stat_time, kwh from (select * from %s where %s_id = %s  and stat_time < "%s" order by stat_time desc, stat_time limit %s) t order by stat_time;' % ( table, comp, company_id, time_end, point)
        if use_kwhi:
            sql = 'select stat_time, kwhi from (select * from %s where %s_id = %s  and stat_time < "%s" order by stat_time desc, stat_time limit %s) t order by stat_time;' % (table, comp, company_id, time_end, point)
    return sql


def mksql_today_sum(company_id='2', time_start='', time_end='', comp='company'):
    import time
    table = '' or 'elec_company_15min_%s' % time.strftime('%Y', time.localtime())
    time_start = '' or fm_tm(fm = '%Y-%m-%d 00:00:00')
    time_end = ''  or  fm_tm()
    sql = 'select max(stat_time), sum(kwh) from \
    (select * from elec_%s_15min_%s where %s_id = %s  and stat_time >= "%s" \
    and stat_time < "%s" order by stat_time ) t order by stat_time;' \
    % (comp ,time.strftime('%Y', time.localtime()), comp, company_id, time_start, time_end)
    return sql


def get_15m_last_n(company_id, point=3, app='mysql:app_eemsop', use_kwhi=True, comp='company'):
    worker = mysql_worker_d[app]
    # sql_example = 'select * from elec_company_15min_2016 where company_id = 12 order by stat_time desc limit 5;'
    # sql = 'select * from %s where company_id=%s order by %s desc limit %s;' % (table_name, company_id, order_by, n)
    sql = mksql_last_15_min(company_id, point, use_kwhi=use_kwhi, comp=comp)
    res = worker(sql)
    return res


def get_sum_today(company_id, app='mysql:app_eemsop', time_start='', time_end='', comp='company'):
    worker = mysql_worker_d[app]
    # sql_example = 'select * from elec_company_15min_2016 where company_id = 12 order by stat_time desc limit 5;'
    # sql = 'select * from %s where company_id=%s order by %s desc limit %s;' % (table_name, company_id, order_by, n)
    sql = mksql_today_sum(company_id, time_start=time_start, time_end=time_end, comp=comp)
    res = worker(sql)
    return res


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


def wns(lst, fold=1, n=1, both_side=True):
    for i in range(int(fold)):
        lst = wnd(lst, n=n, both_side=both_side)
    return lst


def plot_company_line(res, fold=3, line_option='', fee = False, ori = False, show=True, pic_name = ''):
    elcs = [i[2] for i in res]
    fees = [i[4] for i in res]
    xs = [x for x in range(len(res))]
    plt.plot(xs, wns(elcs, int(fold)), line_option)
    if ori:
        plt.plot(xs, wns(elcs, 0), line_option)
    if fee:
        plt.plot(xs, wns(fees), line_option)
        plt.plot(xs, wns(fees, 3), line_option)
    plt.xlabel('Time')
    plt.ylabel('Kw')
    if show:
        plt.show()
    if pic_name:
        plt.savefig(pic_name)
        plt.close()
    return len(res)


def r_set_lst_keys(kv_lst, time_out=2700, app='redis:webpage'):
    r = redis_cursor_d[app]
    p = r.pipeline(transaction=False)
    # sort by time stamp, near ones first.
    import itertools
    if type(kv_lst) == dict:
        kv_lst = [[k, v] for k, v in kv_lst.iteritems()]
    else:
        kv_lst = sorted(kv_lst)
    for pair in kv_lst:
        try:
            k, v = pair
            p.setex(str(k), v, int(time_out))
        except:
            # print('error in writing.')
            import sys
            print(str(sys.exc_info()))
            # print(k , v)
            continue
    p.execute()
    if kv_lst:
        return 1
    else:
        return 0



def tree_to_one(a, b, c, ex = 0.618, positive=True):
    # print('ex is %s' % ex)
    ab = float(b) - a
    bc = c - float(b)
    delta = (bc - ab)
    cd = (bc ) * ex
    d1 = cd + c
    if  d1 < 0 and positive:
        return 0
    else:
        return d1


def test_tree_to_one(company_id=2, n = 96,  ex=0.618, do_plot = True):
    ys = get_15m_last_n(company_id, n)
    zs = [int(i[1]) for i in ys]
    xs = []
    ps = []
    n = 0
    a = b = c = d = 0
    for z in zs:
        a = b
        b = c
        c = d
        d = z
        p = z if n<3 else tree_to_one(a, b, c, ex=ex)
        ps += [p]
        xs += [n]
        n += 1
    if do_plot:
        from matplotlib import pyplot as plt
        plt.plot(xs, zs, color = 'r')
        plt.plot(xs, ps)
        plt.show()
    zps = [zs, ps]
    resi = sum([abs(i - j) for i, j in zip(zps[0], zps[1])])
    print(ex, resi)
    return zps


def residule(company_id=2, n = 96,  ex = 0.618 ):
    zps = test_tree_to_one(company_id, n, ex, do_plot = False)
    resi = sum([abs(i - j) for i, j in zip(zps[0], zps[1])])
    return resi


def resi_vs_ex(company_id=2, n = 96):
    # comp2: 0.4,
    xs = []
    rs = []
    for i in range(100):
        r = residule(company_id=company_id, n=n, ex = i * 0.01)
        rs.append(r)
        xs.append(i)
    plt.plot(xs, rs)
    plt.show()
    xr = zip(xs, rs)
    xrs = sorted(xr, key = lambda d: d[1])
    # print(xrs[0])
    return xrs[0]


# example
r_k_example = 'web_eemsop_company_12_kwh:2016-12-23_171500'

def pred_forward(last_rcds,kwh_sum,comp='company',
                 pred_d={},sec=2700,app_redis_key_word='eemsop',company_id=2,fake_now='', **para_d):
    """use power prediction. last_rcds should have time-value pair.
    last_rcds is 3 records, kwh_sum is the sum.
    """
    import time
    # comp_id='company_%s' % (company_id)
    def int_fo_s(s):
        return time.mktime(time.strptime(s, '%Y-%m-%d %H:%M:%S'))
    def s_fo_int(i):
        return time.strftime('%Y-%m-%d_%H%M%S', time.localtime(i))
    def pk(s):
        return 'web_%s_%s_%s_kwh:%s' % (app_redis_key_word, comp, company_id, s)
    if not last_rcds: return {}
    if len(last_rcds) == 3 and (int_fo_s(str(last_rcds[2][0])) - int_fo_s(str(last_rcds[0][0]))) == 1800:
        print(last_rcds)
        next_kwh = tree_to_one(*[i[1] for i in last_rcds][-3:], **para_d)
        p_next = next_kwh / float(900)
    else:
        next_kwh = last_rcds[-1][1]
        p_next = next_kwh / float(900)
    last_kwh = last_rcds[-1][1]
    p_last = last_kwh / float(900)
    p_delta = p_next - p_last
    t_rcds_end = str(last_rcds[-1][0])
    t_int_rcds_end = int_fo_s(t_rcds_end)
    if not pred_d:
        pred_d = {}
        # here have to use a use_kwhi dict.
        # Notice, have to use init empty dict.
    t_now = fm_tm() if not fake_now else str(fake_now)
    t_int_pred_start = int_fo_s(t_now) + 3
    # delay of 3 s.
    t_pred_start = s_fo_int(t_int_pred_start)
    last_pred_sum = kwh_sum[0][1] if not pk(t_rcds_end) in pred_d else pred_d[pk(t_rcds_end)]
    now_pred_sum = (kwh_sum[0][1] + (t_int_pred_start - t_int_rcds_end) * p_last) if not pk(t_pred_start) in pred_d else pred_d[pk(t_pred_start)]
    diff_sum = int(kwh_sum[0][1]) - last_pred_sum
    p_diff_sum = (diff_sum / float(3.0)) / 900
    # fix third of the difference.
    # p = p_next +  p_delta
    print("last_kwh: %s, p_last:%s, next_kwh:%s, p_next:%s, p_diff_sum:%s, p_delta:%s" % (last_kwh, p_last, next_kwh, p_next, p_diff_sum, p_delta))
    print(int(int_fo_s(t_rcds_end) + sec - t_int_pred_start))
    for i in range(int(int_fo_s(t_rcds_end) + sec - t_int_pred_start)) :
        t_c = t_int_pred_start + i
        add = (i + t_int_pred_start - t_int_rcds_end) * p_delta * 2 / float(900) + p_diff_sum + p_last
        addx = (abs(add) + add) / float(2)
        # if addx == 0:
        #     print('addx 0')
        # this will never be nagitive
        now_pred_sum += addx
        pred_d[pk(s_fo_int(t_c))] = now_pred_sum
    return pred_d


def pred_one(company_id=2, app='mysql:app_eemsop',
             comp='company', app_redis_key_word='eemsop', use_kwhi=False, company_id_map=0):
    """
    company_id,
    comp,
    app_redis_key_word,
    use_kwhi, use kwhi instead of kwh.
    company_id_map,
    """
    if not company_id_map: company_id_map = company_id
    last_15m = get_15m_last_n(company_id, app=app, use_kwhi=use_kwhi, comp=comp)
    print('last_15m', last_15m)
    sum_today = get_sum_today(company_id, app=app, comp = comp)
    print('sum_today', sum_today)
    if sum_today == ((None, None),):
        return [company_id, {}]
    pred_d = pred_forward(last_15m, sum_today, app_redis_key_word=app_redis_key_word, company_id=company_id_map, comp=comp)
    # print(pred_d)
    r_set_lst_keys(pred_d)
    import time
    time.sleep(0.6)
    # return pred_one(app=app, comp=comp, company_id=company_id)
    # import sys
    # args = sys.argv
    # n = 0
    # for arg in args:
    #     import re
    #     if re.match('^--?(\w*)', arg):
    #         va = ''
    #     print(arg)
    return [company_id, pred_d]


def how_about_sleep(shift=480, interval=900, real_sleep=True):
    sleep_for_i = interval - (int(time.time()) % interval) + int(shift)
    sleep_for = (sleep_for_i + abs(sleep_for_i)) / 2
    print('sleeping for %s' % (sleep_for))
    end_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() + sleep_for))
    print(end_time_str)
    if real_sleep:
        time.sleep(sleep_for)
    return sleep_for


def main():
    """
    [[app, comp, id_lst], []...]
    """
    sopw = mysql_worker_d['mysql:app_eemsop']
    company_all = sopw("select id from company;")
    workshop_all = sopw("select id from workshops;")
    cnt = 0
    while True:
        cnt += 1
        # pred_one(company_id=26, app='mysql:app_eemscr', app_redis_key_word='eemsop', use_kwhi=False, company_id_map=2026)
        # rlt25 = pred_one(company_id=2025, app='mysql:app_eemsop', app_redis_key_word='eemsop')
        for i in company_all:
            print(int(i[0]))
            pred_one(company_id=int(i[0]), app='mysql:app_eemsop', app_redis_key_word='eemsop', comp='company')
        for j in workshop_all:
            print(int(j[0]))
            pred_one(company_id=int(i[0]), app='mysql:app_eemsop', app_redis_key_word='eemsop', comp='workshop')
        if cnt > 5:
            company_all = sopw("select id from company;")
            workshop_all = sopw("select id from workshops;")
        print('sleeping for 600')
        # time.sleep(600)
        how_about_sleep()


# TODO: workshop should be in same time.
if __name__ == '__main__':
    main()
