# -*- coding: utf-8 -*-
#!/usr/bin/env python

import read_config
import redis_pool
import mysql_pool

from matplotlib import pyplot as plt
import numpy as np
import scipy as sp

import time, os, sys


dbini = read_config.ReadConfig_DB('../config/db.ini')
dbini_d = dbini.check_config(db_type='mysql', convert_port=True)

r_webpage_pool = redis_pool.RedisWrapper(**dbini_d['redis:webpage'])
r = r_webpage_pool.get_cursor()

sop_pool = mysql_pool.MySQLWrapper(**dbini_d['mysql:app_eemsop'])
sopw = sop_pool.do_work

sec_pool = mysql_pool.MySQLWrapper(**dbini_d['mysql:app_eemssec'])
secw = sec_pool.do_work
syd_pool = mysql_pool.MySQLWrapper(**dbini_d['mysql:app_eemsyd'])
sydw = syd_pool.do_work
scr_pool = mysql_pool.MySQLWrapper(**dbini_d['mysql:app_eemscr'])
scrw = syd_pool.do_work
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


def mksql_last_15_min(comp='2', point=3, table='', time_end='', simple_lst=True):
    import time
    table = '' or 'elec_company_15min_%s' % time.strftime('%Y', time.localtime())
    example_sql = 'select * from (select * from elec_company_15min_2016 where company_id = 2  and stat_time < "2016-12-23 17:45:00" order by stat_time desc, stat_time limit 9600) t order by stat_time;'
    time_end = time_end or fm_tm()
    sql = 'select * from (select * from %s where company_id = %s  and stat_time < "%s" order by stat_time desc, stat_time limit %s) t order by stat_time;' % (table, comp, time_end, point)
    if simple_lst:
        sql = 'select stat_time, kwh from (select * from %s where company_id = %s  and stat_time < "%s" order by stat_time desc, stat_time limit %s) t order by stat_time;' % (table, comp, time_end, point)
    return sql


def mksql_today_sum(comp='2', table='', time_start='', time_end=''):
    import time
    table = '' or 'elec_company_15min_%s' % time.strftime('%Y', time.localtime())
    time_start = '' or fm_tm(fm = '%Y-%m-%d 00:00:00')
    time_end = ''  or  fm_tm()
    sql = 'select max(stat_time), sum(kwh) from \
    (select * from elec_company_15min_2016 where company_id = %s  and stat_time > "%s" \
    and stat_time < "%s" order by stat_time ) t order by stat_time;' \
    % (comp, time_start, time_end)
    return sql


def get_15m_last_n(comp, point=3, table='', worker=syd_pool):
    # sql_example = 'select * from elec_company_15min_2016 where company_id = 12 order by stat_time desc limit 5;'
    # sql = 'select * from %s where company_id=%s order by %s desc limit %s;' % (table_name, company_id, order_by, n)
    sql = mksql_last_15_min(comp, point, table)
    res = worker.do_work(sql)
    return res


def get_sum_today(comp, worker=syd_pool, time_start='', time_end='', table=''):
    # sql_example = 'select * from elec_company_15min_2016 where company_id = 12 order by stat_time desc limit 5;'
    # sql = 'select * from %s where company_id=%s order by %s desc limit %s;' % (table_name, company_id, order_by, n)
    sql = mksql_today_sum(comp, point, table)
    res = worker.do_work(sql)
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


def r_set_keys(kv_lst, time_out=300, r_pool=r_webpage_pool):
    r = r_pool.get_cursor()
    for pair in kv_lst:
        try:
            k, v = pair
            set(k, v, ex=int(time_out))
        except:
            continue
    return True


def tree_to_one_v0(a, b, c, yita = 0.5, ratio=0.75, positive=True):
    ab = float(b) - a
    bc = c - float(b)
    cd = bc * (1 + yita) - ab * yita
    d1 = c + cd
    d2 = c
    if  (d1 * ratio + d2 * (1  - ratio)) < 0 and positive:
        return 0
    else:
        return (d1 * ratio + d2 * (1 - ratio))


def tree_to_one_v2(a, b, c, yi=0, ex = 0.618, positive=True):
    # TODO: 1, should test another funtion, and weighted minimum diff method.
    # TODO: 2, the point should not start from center, but right.
    ab = float(b) - a
    bc = c - float(b)
    delta = bc - ab
    # TODO: not right here.s
    cd = (bc + delta * yi) * ex
    d1 = cd * 2 + (a + b + c) / 3
    if  d1 < 0 and positive:
        return 0
    else:
        return d1


def tree_to_one(a, b, c, yi=0, ex = 0.618, positive=True):
    print('ex is %s' % ex)
    ab = float(b) - a
    bc = c - float(b)
    delta = bc - ab
    cd = (bc + delta * yi) * ex
    d1 = cd + c
    if  d1 < 0 and positive:
        return 0
    else:
        return d1


def test_tree_to_one_v2(lst=[1,1,4], n=10, yi=0, ex = 0.618):
    a, b, c = lst
    for i in range(n):
        d = tree_to_one(a, b, c, yi, ex)
        a = b
        b = c
        c = d
        lst.append(d)
    from matplotlib import pyplot as plt
    xs = [i for i in range(n + 3)]
    plt.plot(xs, lst)
    plt.show()
    return lst


def test_tree_to_one(comp=2, n = 96, yi = 0.0, ex=0.618, do_plot = True):
    ys = get_15m_last_n(comp, n)
    zs = [int(i[2]) for i in ys]
    xs = []
    ps = []
    n = 0
    a = b = c = d = 0
    for z in zs:
        a = b
        b = c
        c = d
        d = z
        p = z if n<3 else tree_to_one(a, b, c, yi = yi, ex=ex)
        ps += [p]
        xs += [n]
        n += 1
    if do_plot:
        from matplotlib import pyplot as plt
        plt.plot(xs, zs, color = 'r')
        plt.plot(xs, ps)
        plt.show()
    return [zs, ps]


def residule(comp=2, n = 96, yi = -0.5, ex = 0.618 ):
    zps = test_tree_to_one(comp, n, yi, ex, do_plot = False)
    resi = sum([abs(i - j) for i, j in zip(zps[0], zps[1])])
    return resi


def resi_vs_ex(comp=2, n = 96, yi = 0.0):
    # yi = 00
    # comp2: 0.4,
    xs = []
    rs = []
    for i in range(100):
        r = residule(comp=comp, n=n, yi=yi, ex = i * 0.01)
        rs.append(r)
        xs.append(i)
    plt.plot(xs, rs)
    plt.show()
    xr = zip(xs, rs)
    xrs = sorted(xr, key = lambda d: d[1])
    print(xrs[0])
    return xrs[0]


def resi_vs_yi(comp=2, n = 96, ex = 0.618):
    # 0.57
    xs = []
    rs = []
    for i in range(100):
        r = residule(comp=comp, n=n, ex = ex, yi = (i * 0.02 -1))
        rs.append(r)
        xs.append(i * 0.02 -1)
    plt.plot(xs, rs)
    plt.show()
    return [xs, rs]


# example
r_k = 'web_eemsyd_company_12_kwh/2016-12-23_171500:'

def pred_forward(last_recds,kwh_sum,sec=1500,pred_d={},app_type='eemsyd',comp_id='company_2',fake_now='', **para_d):
    """use power prediction. last_recds should have time-value pair.
    last_recds is 3 records, kwh_sum is the sum.
    """
    import time
    def int_fo_s(s):
        return time.mktime(time.strptime(s, '%Y-%m-%d %H:%M:%S'))
    def s_fo_int(i):
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(i))
    def pk(s):
        return 'web_%s_%s_kwh/%s' % (app_type, comp_id, s)
    if len(last_recds) == 3 and (int_fo_s(str(last_recds[2][0])) - int_fo_s(str(last_recds[0][0]))) == 1800:
        next_kwh = tree_to_one(*[i[1] for i in last_recds][-3:], **para_d)
        p_next = next_kwh / float(900)
    else:
        next_kwh = last_recds[-1][1]
        p_next = next_kwh / float(900)
    last_kwh = last_recds[-1][1]
    p_last = last_kwh / float(900)
    p_delta = p_next - p_last
    t_rcds_end = str(last_recds[-1][0])
    if not pred_d:
        pred_d = {}
        # here have to use a new dict.
    t_now = fm_tm() if not fake_now else str(fake_now)
    t_int_pred_start = int_fo_s(t_now) + 3
    # delay of 3 s.
    t_pred_start = s_fo_int(t_int_pred_start)
    kwh_now =  kwh_sum[1] if not pk(t_pred_start) in pred_d else pred_d[pk(t_pred_start)]
    # if pk(t_pred_start) in pred_d:
    #     print('it is found in the pred_d')
    diff_sum = int(kwh_sum[1]) - kwh_now
    p_diff = (diff_sum / float(3.0)) / 900
    # fix third of the difference.
    # p = p_next +  p_delta
    print(last_kwh, p_last, next_kwh, p_next, p_diff)
    for i in range(int(int_fo_s(t_rcds_end) + sec - t_int_pred_start)) :
        t_c = t_int_pred_start + i
        add = i * p_delta * 2 / float(900) + p_diff + p_last
        add = 0 if add < 0 else add
        # this will never be nagitive
        kwh_now += add
        pred_d[pk(s_fo_int(t_c))] = kwh_now
    return pred_d

def main():
    for i in range(27):
        bk = plot_company_line(sydw(mksql_last_15_min(comp=i + 1, point=360*96)), show=False, pic_name='/home/lcg/Pictures/eems/yd_copany_history_%s.jpeg' % (i+1))
        print(i+1, bk)
    pass

if __name__ == '__main__':
    main()
