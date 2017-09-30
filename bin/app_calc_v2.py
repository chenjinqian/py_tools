#!/usr/bin/env python
# Author: chenjinqian
# Email:  2012chenjinqian@gmail.com

"""
This script will get meter data from redis cache, with information
of company/workshop and meter gmid map, calculate out variable such as
kwh, charge and p etc, write those in mysql database.

In redo model, script will run one time, work on give time points.
This is usefull when some data is late, or wrong at first time,
and the calculation need to be redo.

In main model, programe will calculate several time point within 4h,
and wake up every interval time. Interval time is 15m in most case,
1m is also possible in some case.

TODO:
interval = 900
redo mode related_meters parse.
pritn_level
"""

import tools.read_config as rcfg
import tools.parse_argv
import tools.mysql_pool as mpol
import redis
import os, re, sys, time
import copy
from decimal import Decimal as DFloat
import itertools
try:
    this_path = os.path.realpath(os.path.dirname(__file__))
except:
    try:
        this_path = os.path.realpath(os.path.dirname('__file__'))
    except:
        this_path = '/app/bin/'

try:
    import tools.hzlogger as hzlog
    hlog = hzlog.get_eems_log(os.path.join(this_path, '../log/'),
                              "app_calc",
                              'error')
    have_hzlog = True
except:
    have_hzlog = False


#######      GLOBAL CONFIG     #######
DB_INI_PATH = os.path.join(this_path, '../config/db.ini')
REDIS_NAME = 'redis:meter'
INTERVAL = 900
# """check-point _interval, should match with redis key structure.
# 15 minutes for normal task,
# zdxl(maxmum need amount) should be 1 minute, 60s"""
SECONDS_ONE_HOUR = 3600
BAD_POINT_THRESHOLD = [5, 100, 50]
# """limit, ratio, have_points"""
REDIS_SYNC_KEY = 'sync_meterinfo'
VARIABLE_TYPE = {"kwhttli":0,
                 "kwhttle":0,
                 "pttl":2,
                 "kvarhttli":0,
                 "kvarhttle":0,
                 "qttl":2}

#  VARIABLE_TYPE = [
#  ['kwhttli', 0],
#  ['kwhttle', 0],
#  ['pttl', 2],
#  ['kvarhttli', 0],
#  ['kvarhttle', 0],
#  ['qttl', 2]]

CKPS = [0, 60*15*1, 60*15*4, 60*15*9, 60*15*16]
CKPS_INIT = [0, 60*15*1, 60*15*2, 60*15*4, 60*15*6, 60*15*8,
             60*15*10, 60*15*12, 60*15*14, 60*15*16]
PTTL_FILTER = True

#######  END of GLOBAL CONFIG  #######


class AppSqlInfo(object):
    """
    All mysql related info and operation function here.
    method offered:
    self.get_map_d()
    self.get_meta_d()
    self.chose_company([company_list=])
    self.get_meter(app, comp, cid, ckp)
    self.get_price(app, comp, cid, ckp)
    self.sql_op(info_dict)
    self.clear_mysql_pool()
    """
    def __init__(self, app):
        self.app = app
        self.ini=DB_INI_PATH
        self._make_mysql_worker_d()
        self.nocp = False
        self.get_map_d()
        self.get_meta_d()


    def _make_mysql_worker_d(self):
        """make_mysqlpool_dictory,
        read config files,
        and make mysql connection pool instance as dict."""
        _s = rcfg.ReadConfig_DB(self.ini)
        cfgd = _s.check_config(db_type='mysql', convert_port=True)
        pol = mpol.MySQLWrapper(**cfgd[self.app])
        worker = mpol.MySQLWrapper(**cfgd[self.app]).do_work
        self.pol = pol
        self.worker = worker
        return worker


    def get_map_d(self):
        rst_d = {}
        sql1 = "select id, company_id from workshops;"
        sql2 = "select id from company;"
        rst_d[self.app] = {}
        company_ids = self.worker(sql2)
        wid_and_related_cid = self.worker(sql1)
        # wid for workshop_id, cid for company_id
        if company_ids is None or wid_and_related_cid is None:
            self.map_d_full = copy.deepcopy(rst_d)
            self.map_d = copy.deepcopy(rst_d)
            return rst_d
        for i in company_ids:
            rst_d[self.app]["company/%s" % int(i[0])] = {}
        for lst in wid_and_related_cid:
            wid, cid = lst
            if not 'company/%s'%cid in rst_d[self.app]:
                rst_d[self.app]["company/%s"%cid] = {}
            rst_d[self.app]['company/%s' % int(cid)][str("workshop/%s"%(wid))] = ''
        self.map_d_full = copy.deepcopy(rst_d)
        self.map_d = copy.deepcopy(rst_d)
        return rst_d


    def get_meta_d(self):
        """
        meta_info of all company and workshop,
        related gmids and mysql database config info
        in this eems app, ordered in the form of dictory.
        example:
        ...
        """
        rst_d = {}
        d1 = self._get_meta_acc()
        d2 = self._get_meta_acc(comp = 'workshop')
        for k in d1:
            rst_d[k] = d1[k]
        for k in d2:
            rst_d[k] = d2[k]
        self.meta_d = rst_d
        return rst_d


    def _get_meta_acc(self, comp='company', patch_workshop=True):
        """
        Get all company info, or workshop info, as dictory,
        which could be merged to form full meta_info.
        'patch_Workshop' means convert from 'workshop' to 'workshops' to
        adapt mysql database table name mis-match, which is a
        history problem.
        """
        s = RedisInfo()
        mid_meta_d = s.get_mid_meta_d()
        company_id_d = {}
        rst_d = {}
        if (patch_workshop and comp=='workshop'):
            comp_for_sql = 'workshops'
            # patch for mysql table name unmatch.
        else:
            comp_for_sql = comp
        try:
            cid_list = self._sql_get_info(comp_for_sql,
                                         0,
                                         option = 'company_id')
            rst_d['%s/%s' % (self.app, comp)] = {}
            for cid in cid_list:
                rst_d['%s/%s' % (self.app, comp)]['%s' % cid] = {}
        except Exception as e:
            print('#13, ERROR:%s'%s(repr(e)))
        for ap_comp in rst_d:
            # app, comp = ap_comp.split('/')
            for cid in rst_d[ap_comp].keys():
                rst_d[ap_comp][cid] = {}
                meter_config = self._sql_get_info(comp_for_sql,
                                                 cid,
                                                 option='meter_config')
                rst_d[ap_comp][cid]['meter_config'] = meter_config
                try:
                    meter_lst = self._get_config_meter_id_all(meter_config)
                except Exception as e:
                    print("#5, ERROR:%s\nap_comp:%s,cid:%s"%(repr(e),
                                                             ap_comp,
                                                             cid))
                    meter_lst = []
                    # get related meters at the time of this very current mement.
                rst_d[ap_comp][cid]['meter'] = {}
                for mid in meter_lst:
                    if str(mid) in mid_meta_d:
                        _s = mid_meta_d[str(mid)]
                    else:
                        _s = {'use_power':'1',
                              'use_energy':'0'}
                    rst_d[ap_comp][cid]['meter'][str(mid)] = _s
        for ap_comp in rst_d:
            # app, comp = ap_comp.split('/')
            for cid in rst_d[ap_comp]:
                _s2 = self._sql_get_info(comp_for_sql,
                                       cid,
                                       option='price_config')
                rst_d[ap_comp][cid]['price_config'] = _s2
                try:
                    _s3 = self._chose_from_prices_at_ckp(_s2, 0)
                    _s4 = self._convert_price_format(_s3)
                    # get price policy at time of this very current mement.
                except Exception as e:
                    print("#6, config is wrong, %s/%s"%(ap_comp, cid))
                    _s4 = {}
                rst_d[ap_comp][cid]['price'] = _s4
        return rst_d


    def _sql_get_info(self, comp, cid, option=''):
        # TODO: change this function name to _sql_get_info
        # TODO: meter_time deleting
        if option == 'price_config':
            return self._sql_get_price_config(comp, cid)
        elif option == 'company_id':
            return self._sql_get_company_id(comp, cid)
        elif option == 'workshop_id':
            return self._sql_get_workshop_id(comp, cid)
        elif option == 'equipment_id':
            return self._sql_get_equipment_id(comp, cid)
        else:
            option = 'meter_config'
            return self._sql_get_meter_config(comp, cid)


    def _sql_get_price_config(self, comp, cid):
        sql_describe = 'describe price_policy;'
        rlt_describe = self.worker(sql_describe)
        ss_spfv = [str(s[0].split('_')[1])
                   for s in rlt_describe
                   if 'price_' in s[0]]
        if comp == 'company':
            if 's' in ss_spfv:
                sql = 'select hours, \
                price_p, \
                price_f, \
                price_v,\
                price_s, \
                start_date  \
                from price_policy  \
                where %s_id=%s \
                order by start_date desc' % (comp, cid)
            else:
                sql = 'select hours, \
                price_p, \
                price_f, \
                price_v, \
                start_date \
                from price_policy  \
                where %s_id=%s \
                order by start_date desc' % (comp, cid)
        else:
            sql_get_real_cid = 'select company_id \
            from %s where id = %s' % (comp, cid)
            tmp = self.worker(sql_get_real_cid)
            real_cid  = tmp[0] if tmp else ''
            if real_cid:
                if 's' in ss_spfv:
                    sql = 'select hours, \
                    price_p, \
                    price_f, \
                    price_v, \
                    price_s, \
                    start_date  \
                    from price_policy  \
                    where company_id=%s \
                    order by start_date desc' % (real_cid)
                else:
                    sql = 'select hours, \
                    price_p, \
                    price_f, \
                    price_v, \
                    start_date \
                    from price_policy  \
                    where company_id=%s \
                    order by start_date desc' % (real_cid)
            else:
                return {}
        rst = self.worker(sql)
        return rst


    def _sql_get_company_id(self,  comp, cid):
        sql = 'select id from %s;' % comp
        try:
            rst = self.worker(sql)
        except:
            print('sql query my_except')
            return []
        if rst:
            rst_int = [int(i[0]) for i in rst]
        else:
            rst_int = []
        return rst_int


    def _sql_get_workshop_id(self,  comp, cid):
        sql = 'select id from workshop;'
        # database reason, here use workshop, not workshops
        try:
            rst = self.worker(sql)
        except:
            print('sql query my_except')
            return []
        if rst:
            rst_int = [int(i[0]) for i in rst]
        else:
            rst_int = []
        return rst_int


    def _sql_get_equipment_id(self,  comp, cid):
        sql = 'select id from equipment;'
        try:
            rst = self.worker(sql)
        except:
            print('sql query my_except')
            return []
        if rst:
            rst_int = [int(i[0]) for i in rst]
        else:
            rst_int = []
        return rst_int


    def _sql_get_meter_config(self, comp, cid):
        """
        return that long string of
        related meters config in mysql database.
        """
        sql = 'select related_meters from %s where id=%s;' % (comp, cid)
        rst = self.worker(sql)
        return rst


    def _chose_from_prices_at_ckp(self, price_list_raw, ckp):
        """
        chose one from prices which is like
        ((hours, ..., datetime.date(2016,6,1)), (...), ...),
        and str(datetime.date(...)) is like '2016-06-01'
        return a list with one chosed policy inside
        or empty list.
        example:
            rlte = \
            ((u'vvvvvvvvfpppfffffpppppfv',
            DFloat('0.8571'),
            DFloat('0.5759'),
            DFloat('0.3663'),
            datetime.date(2016, 10, 1)),
            (u'vvvvvvvvfpppfffffpppppfv',
            DFloat('0.9082'),
            DFloat('0.5759'),
            DFloat('0.3663'),
            datetime.date(2016, 7, 1)),
            (u'vvvvvvvvfpppfffffpppppfv',
            DFloat('0.8571'),
            DFloat('0.5759'),
            DFloat('0.3663'),
            datetime.date(2016, 6, 24)),
            (u'vvvvvvvvfpppfffffpppppfv',
            DFloat('0.8571'),
            DFloat('0.5759'),
            DFloat('0.3663'),
            datetime.date(2015, 10, 1)))
        result:
            (today is 2017-06-23)
            _chose_from_prices(rlte) =>
            [(u'vvvvvvvvfpppfffffpppppfv',
            0.8571,
            0.5759,
            0.3663,
            datetime.date(2015, 10, 1))]
        """
        dates_all = [str(i[-1]) for i in price_list_raw]
        now_year, now_month, now_day = time.strftime(
            '%Y-%m-%d', time.localtime(
                time.time() - int(ckp))).split('-')
        da_last = ''
        end = False
        dates_filted = []
        for da in dates_all:
            if str.split(da, '-')[0] > now_year:
                dates_filted.append(None)
                continue
            if not da_last:
                da_last = da.split('-')[0]
            if da.split('-')[0] == da_last:
                dates_filted.append(da)
            else:
                if not end:
                    end = True
                    dates_filted.append(da)
                else:
                    dates_filted.append(None)
        now_md = "%s-%s"%(now_month, now_day)
        dates_first = []
        not_first_flag = True
        dates_chosen = []
        not_chosed_flag = True
        year_a = ''
        for dfd in dates_filted:
            if dfd:
                year_b = dfd.split('-')[0]
                dfd_md = "%s-%s" % (
                    dfd.split('-')[1],
                    dfd.split('-')[2])
                if dfd and not_first_flag:
                    dates_first.append(dfd)
                    not_first_flag = False
                else:
                    dates_first.append(None)
                if (not_chosed_flag
                   and (now_md >= dfd_md
                        or (year_a
                            and not year_a == year_b))):
                    dates_chosen.append(dfd)
                    not_chosed_flag = False
                else:
                    dates_chosen.append(None)
                year_a = year_b
            else:
                dates_chosen.append(None)
        lst_chosen = [i[0]
                      for i in
                      zip(price_list_raw, dates_chosen)
                      if i[1]]
        lst_first  = [i[0]
                      for i in
                      zip(price_list_raw, dates_first)
                      if i[1]]
        lst_one = lst_chosen or lst_first
        return lst_one


    def _get_config_meter_id_all(self, meter_config_raw):
        """
        get all possible meter id, as list
        """
        try:
            config_list =  [i for i in meter_config_raw[0][0].split('/') if i]
            rst_list = [[i.split(':')[0],
                         [k for k in i.split(':')[1].split(',') if k]]
                        for i in config_list]
            tmp_d = {}
            for item_one in rst_list:
                time_str, mid_list = item_one
                for mid_one in mid_list:
                    try:
                        tmp_d[str(int(mid_one))] = ''
                    except:
                        continue
            mids = sorted(tmp_d.keys())
            return mids
        except Exception as e:
            print("#31, meter config error: %s"%(repr(e)))
            return []


    def chose_company(self, wanted_comp_list):
        """
        chose company certain from map_d_full,
        which's usefull in redo-mode, if only
        want redo those companys within cids.
        example:
          self.chose_company({"mysql:app_eemsii":{'company/6':''}})
        output:
          result = {'mysql:app_eemsii':
            {'company/6': {'workshop/19': '', 'workshop/20': ''}}}
          (and self.map_d = result)
        """
        if not wanted_comp_list:
            return self.map_d
        if not (type(wanted_comp_list) == list):
            wanted_comp_list = [wanted_comp_list]
        new_d = {}
        new_d[self.app] = {}
        wanted_comp_d = {}
        wanted_comp_d[self.app] = {}
        for cid in wanted_comp_list:
            wanted_comp_d[self.app]["company/%s"%(cid)] = ''
        for comp_cid in self.map_d_full[self.app]:
            if comp_cid in wanted_comp_d[self.app]:
                new_d[self.app][comp_cid] = self.map_d_full[self.app][comp_cid]
            else:
                continue
        self.map_d = new_d
        return new_d


    def get_meter(self, comp, cid, ckp):
        """
        get id of related meters,
        at the check_point time,
        as list.
        """
        mids_d = {}
        meter_config_raw = self.meta_d["%s/%s"%(self.app, comp)]["%s"%(cid)]['meter_config']
        try:
            mids = self._chose_from_meters_at_ckp(meter_config_raw, ckp)
            for mid in mids:
                _s = self.meta_d["%s/%s"%(self.app, comp)]["%s"%(cid)]['meter']["%s"%(str(mid))]
                mids_d["%s"%(str(mid))] = _s
        except Exception as e:
            print("#3, ERROR:%s\ncomp:%s, cid: %s, ckp:%s"%(repr(e), comp, cid, ckp))
        return mids_d


    def _chose_from_meters_at_ckp(self, meter_config_raw, ckp):
        """
        TODO:
        input:

        output:

        no exception caption,
        cause no usefull information could be found here.
        """
        mids = []
        config_list =  [i for i in meter_config_raw[0][0].split('/') if i]
        rst_list = [[i.split(':')[0],
                     [k for k in i.split(':')[1].split(',') if k]]
                    for i in config_list]
        rst_list_sorted = sorted(rst_list, key=lambda x: x[0])
        ckp_str = time.strftime("%Y%m%d_%H%M%S",
                                time.localtime(time.time() - int(ckp)))
        itm_last = []
        for itm in rst_list_sorted:
            if itm[0] < ckp_str:
                itm_last = itm
            else:
                break
        if itm_last:
            mids = itm_last[1]
        return mids


    def get_price(self, comp, cid, ckp):
        """
        use meta_d, should not be called before meta_d is formed.
        get price policy from mysql database,
        support season price by using function _chose_from_prices.
        """
        try:
            cid_info_d = self.meta_d["%s/%s"%(self.app, comp)]["%s"%(cid)]
            price_list_raw = cid_info_d['price_config']
            price_one = self._chose_from_prices_at_ckp(price_list_raw, ckp)
            tmp_d = self._convert_price_format(price_one)
        except Exception as e:
            print("#4, ERROR: %s"%(repr(e)))
            return {}
        return tmp_d


    def _convert_price_format(self, price_one):
        """
        TODO: full example here.
        input:(output of self._chose_from_price_at_ckp)
        output:(price dictory)
        """
        tmp_d = {}
        if not price_one:
            print("got illege price policy: %s"%(rst))
            return {}
        h, p, f, v = price_one[0][0:4]
        tmp_d['hours'] = h
        tmp_d['p'] = DFloat(p) if p else DFloat('0.0')
        tmp_d['f'] = DFloat(f) if f else DFloat('0.0')
        tmp_d['v'] = DFloat(v) if v else DFloat('0.0')
        if len(price_one[0]) == 6:
            s = price_one[0][4]
            tmp_d['s'] = DFloat(str(s)) if s else DFloat('0.0')
        return tmp_d



    def sql_op(self, dict_sql):
        """
        info_and_dict is like:
        {'mysql:app_eemsop/company/38/20170118_064500':
          {'_time_sql': '2017-01-18 06:30:00',
          'charge': 62.03528756860898,
          'kvarhi': -17.749406617955692,
          'kwhe': 0.0,
          'kwhi': 166.26986751168315,
          'p': 2660.3178801869303,
          'q': 477.80727717022285,
          'spfv': 'v'},
        ...
        }
        it should go to database app_eemsop,
        table elec_company_15min_2017, as
        _time and other variables.
        """
        sqls = []
        if (not dict_sql) or self.nocp:
            return sqls
        for key_reduce, sd in dict_sql.iteritems():
            if not sd:
                continue
            _app_complex, comp, cid, _time_left = key_reduce.split('/')
            # app_complex is of no use here.
            sql = "insert into elec_%s_15min_%s \
            (stat_time, %s_id, charge,\
            kwhi, kwhe, kvarhi,\
            kvarhe, p, q,\
            spfv, kwh, pi,\
            pe, qi, qe) \
            values \
            ('%s', '%s', '%s',\
            '%s', '%s', '%s',\
            '%s', '%s', '%s',\
            '%s', '%s', '%s',\
            '%s', '%s','%s')\
            on duplicate key update \
            charge='%s', kwhi='%s', kwhe='%s', \
            kvarhi='%s', kvarhe='%s', p='%s', \
            q='%s', spfv='%s', kwh='%s', \
            pi='%s', pe='%s', qi='%s', \
            qe='%s' " % \
            (comp,
             time.strftime('%Y',time.localtime()),
             comp,sd['_time_sql'],
             int(cid),
             sd['charge'],
             sd['kwhi'],
             sd['kwhe'],
             sd['kvarhi'],
             sd['kvarhe'],
             sd['p'],
             sd['q'],
             sd['spfv'],
             sd['kwhi'],
             sd['pi'],
             sd['pe'],
             sd['qi'],
             sd['qe'],
             sd['charge'],
             sd['kwhi'],
             sd['kwhe'],
             sd['kvarhi'],
             sd['kvarhe'],
             sd['p'],
             sd['q'],
             sd['spfv'],
             sd['kwhi'],
             sd['pi'],
             sd['pe'],
             sd['qi'],
             sd['qe'])
            sqls.append(sql)
            try:
                self.worker(sql, commit=True)
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                print("#44, ERROR:%s"%(repr(e)))
                print('except on writing dict: %s' % (dict_sql))
                continue
        return sqls


    def clear_mysql_pool(self):
        """MySQLWrapper defined a method
        to clear the connection pool, using that."""
        try:
            self.worker('', refresh=True)
        except:
            print("#45, error while try to refresh mysql cursor pool.")
            return False
        return True


class RedisInfo(object):
    """
    redis related operation, including
    funtion:
    get_meter_data_at_ckp
    """
    def __init__(self):
        self.refresh_start_time(time.time())
        self._redis_name=REDIS_NAME
        self._ini=DB_INI_PATH
        self._sync_key = REDIS_SYNC_KEY
        self.variable_type = VARIABLE_TYPE
        self._interval = INTERVAL
        self._make_redis_cur_d()


    def refresh_start_time(self, t):
        self.start_time = t
        return t


    def _make_redis_cur_d(self):
        """make_redispool_dictory"""
        mark='redis:'
        _s = rcfg.ReadConfig_DB(self._ini)
        cfgd = _s.check_config(db_type='redis', convert_port=True)
        db = cfgd[self._redis_name]
        r = redis.Redis(**db)
        self.r = r
        return r


    def get_mid_meta_d(self):
        mid_list = self._get_mid_list()
        return self._get_mid_list_info(mid_list)


    def _get_mid_list(self):
        r = self.r
        mids = r.hkeys(self._sync_key)
        return mids


    def _get_mid_list_info(self, mid_list):
        rlt_d = {}
        r = self.r
        p = r.pipeline(transaction=False)
        if type(mid_list) == int:
            mid_list = [mid_list]
        for mid in mid_list:
            p.hget(self._sync_key, mid)
        pip_rlt = p.execute()
        for mid, pli in itertools.izip(mid_list, pip_rlt):
            rlt_d[mid] = self._parse_redis_str(pli)
        return rlt_d


    def _parse_redis_str(self, sss):
        d = {}
        if not sss:
            return d
        ss = [s for s in sss.split(',') if s]
        for s in ss:
            try:
                k, v = s.split('=')
                d[k] = v
            except:
                continue
        return d


    def get_meter_data_at_ckp(self, mid, ckp=0):
        """
        no exception caption.
        v3
        redis keys has changed, not it is like
        {15min_ts: {min_second_var:value, ...}},
        this v2 function is basic same as origin,
        but the return value is not like
        {time_string: all_vrs_long_string, ...},
        it will be like {vrs_1:{ts_int_1: value, ...}, ...},
        this will be easy for kwh_interval to parse.
        but, since we already have one working kwh_interval,
        why not just return same value as v1 does?
        other three value is same. Third value is used
        in one place as meta info now.
        first, for one vrs, get all keys name in redis,
        then get all value/near ckp value, using redis pipline.
        last, make dict, vrs is key, time_int as sub key,
        variable value as dict value.
        ..1
        TODO: fix kwh and pttl replace bug.
        (use_power = 0,  use_energy = 0)
        return all same result
        example:
        {'data_left': {'20170714_110034': 'pttl=429.6,qttl=83.1',
        '20170714_111434': 'kwhttli=609217.5,kwhttle=562.5'},
        'data_right': {'20170714_111534': 'pttl=333.6,qttl=31.2',
        '20170714_111549': 'kwhttli=609225.0,kwhttle=562.5',
        '_time_left':'20170714_110000',
        '_time_right':'20170714_111500'}
        NOTICE: call refresh_start_time() to get newest data.
        """
        p = self.r.pipeline(transaction=True)
        vrs = [i for i in self.variable_type]
        lk1 = self._ts_ckp_int(-self._interval-ckp)[:-2]
        rk1 = self._ts_ckp_int(-ckp)[:-2]
        ### v3
        meter_min_vrs_left =  ['meterdata_%s_%s_%s' %
                               (mid, lk1, vr) for vr in vrs]
        meter_min_vrs_right = ['meterdata_%s_%s_%s' %
                               (mid, rk1, vr) for vr in vrs]
        meter_min_vrs_all = meter_min_vrs_left + meter_min_vrs_right
        for meter_min_vr in meter_min_vrs_all:
            p.hgetall(meter_min_vr)
        rlt_all = p.execute()
        data_dic_l = []
        data_dic_r = []
        for i, j in zip(meter_min_vrs_left, rlt_all):
            data_dic_l.append([i, j])
        empty_lst = [None for i in meter_min_vrs_left]
        for i, j in zip(empty_lst + meter_min_vrs_right, rlt_all):
            if not i :
                continue
            else:
                data_dic_r.append([i, j])
        res_d = {}
        left_rlt_d = self._convert_dict_format(data_dic_l)
        right_rlt_d = self._convert_dict_format(data_dic_r)
        if not left_rlt_d and not right_rlt_d:
            return {}
        res_d['data_left'] = left_rlt_d
        res_d['data_right']= right_rlt_d
        res_d['_time_left'] = self._ts_ckp_int(-self._interval-ckp)
        res_d['_time_right'] = self._ts_ckp_int(-ckp)
        return res_d


    def _ts_ckp_int(self, i):
        ckp_int = time.localtime(int(self.start_time)
                                - (int(self.start_time) % self._interval)
                                + int(i))
        s = time.strftime('%Y%m%d_%H%M%S', ckp_int)
        return s


    def _convert_dict_format(self, data):
        d = {}
        for lst in data:
            meter_min_vrs_s, min_sec_d = lst
            split_it = meter_min_vrs_s.split('_')
            vr = split_it[-1]
            keys_min_sec = min_sec_d.keys()
            for i in keys_min_sec:
                new_key = '%s%s' % ('_'.join(split_it[2:-1])[:-2], i)
                if new_key in d:
                    d[new_key] = '%s,%s=%s' % (d[new_key],
                                               vr,
                                               min_sec_d[i])
                else:
                    d[new_key] = '%s=%s' % (vr,min_sec_d[i])
        return d


class AppCalc(object):
    """
    Get data from redis, and config info (meta_d, map_d)
    from sql database,
    calculate company/workshop charge (kwh, p, charge etc),
    with respect of history record.
    """
    def __init__(self, app):
        self.app = app
        self.comp = 'company'
        self.cid = 0
        self.variable_type = VARIABLE_TYPE
        self.prepare_task_flag = True
        self.seconds_one_hour = SECONDS_ONE_HOUR
        self.bad_point_threshold = BAD_POINT_THRESHOLD
        self.ckps = sorted(CKPS)
        self.ckps_init = sorted(CKPS_INIT)
        self.ckps_loop = sorted(CKPS)
        self.history = {}
        self.sql = AppSqlInfo(self.app)
        self.redis = RedisInfo()


    def set_prepare_task_flag(self):
        self.prepare_task_flag = True
        return True


    def un_set_prepare_task_flag(self):
        self.prepare_task_flag = False
        return False


    def refresh_sql_info(self):
        try:
            self.sql.get_meta_d()
            self.sql.get_map_d()
            return True
        except Exception as e:
            print("#31, ERROR:%s"%(repr(e)))
            return False


    def one_comp_calc(self, comp_cid):
        """
        calculate one company/workshop charge,
        at given check-points.
        ckp is int, represent time shift left from now.
        input:
          'company/76'
        output:
          {'mysql:app_eemsii/company/6/201707019_180000':
            {'_time_sql': '2017-07-19 18:00:00',
            'charge': 62.03528756860898,
            'kvarhi': -17.749406617955692,
            'kwhe': 0.0,
            'kwhi': 166.26986751168315,
            'p': 2660.3178801869303,
            'q': 477.80727717022285,
            'spfv': 'v'}}
        """
        try:
            print("%s"%(comp_cid))
            comp, cid = comp_cid.split('/')
            self._set_company(comp, cid)
            # used by self.get_and_tag_data function.
            tasks = self._make_meter_task(comp, cid)
            if not tasks:
                print("No meter tasks, nothing to do.")
                return {}
            sql_recd_d = {}
            for task in tasks:
                mid, ckp = task
                one_task_sumup = self.one_meter_calc(mid, ckp)
                sql_recd_d = self._company_addup(sql_recd_d, one_task_sumup)
            return sql_recd_d
        except Exception as e:
            print("#12, ERROR:%s"%(repr(e)))
            return {}


    def _make_meter_task(self, comp, cid):
        try:
            tasks = []
            if self.prepare_task_flag:
                ckps_prepare = [int(ckp + self.redis._interval)
                             for ckp in self.ckps]
                ckps_full = ckps_prepare + self.ckps
                for ckp in ckps_full:
                    meters = self.sql.get_meter(comp, cid, ckp)
                    tasks += [[mid, ckp] for mid in meters]
            else:
                for ckp in self.ckps:
                    meters = self.sql.get_meter(comp, cid, ckp)
                    tasks += [[mid, int(ckp)] for mid in meters]
            self.prepare_task_flag = False
            return tasks
        except Exception as e:
            print("#1, ERROR: %s"%(repr(e)))
            return []


    def _set_company(self, comp, cid):
        self.comp = comp
        self.cid = cid
        return True


    def one_meter_calc(self, mid, ckp):
        """
        input:
          ('35334', 900)
          mid: meter id, string.
          ckp: second from checkpoint to now, int
        return:
          {'kvarhttle': Decimal('0.00'),
          'kvarhttli': Decimal('123.01'),
          'kwhttle': Decimal('0.00'),
          'kwhttli': Decimal('283.62'),
          'pttl': [Decimal('286.0540277778'), Decimal('0.0')],
          'qttl': [Decimal('123.7931944444'), Decimal('0.0')],
          '_time_left': '201707019_180000',
          '_time_right': '201707019_181500',
          '_time_sql': '2017-07-19 18:00:00',
          'key_left': 'mysql:app_eemsii/company/6/35334/201707019_180000',
          'key_right': 'mysql:app_eemsii/company/6/35334/201707019_181500'}
          'key_reduce': 'mysql:app_eemsii/company/6/201707019_180000',
          'ckp':900
        NOTICE:
          self.company and self.cid matters to result.
          should set up comany id before call this function,
          or history record will be not right.
          (set to company 0 or last self.cid valeu)
        TODO:
          try exception, return {} if any error happens.
        """

        data_d = self.get_and_tag_data(mid, ckp)
        self._update_ckp_history(data_d)
        try:
            sumup = self.sumup(data_d)
        except Exception as e:
            # print('#9, ERROR:%s'%(repr(e)))
            sumup = {}
        self._delete_old_history(data_d)
        return sumup


    def get_and_tag_data(self, mid, ckp):
        """
        add extral tags to one piece of data,
        such as "_key_left", "_time_sql"
        with app,  company-id infomation,
        so that this data could be used in history
        and add up to company / workshop.
        """
        data_d = self.redis.get_meter_data_at_ckp(mid, ckp)
        if not data_d:
            return {}
        key_left = '%s/%s/%s/%s/%s' % \
                   (self.app, self.comp, self.cid,
                    mid, data_d['_time_left'])
        key_right = '%s/%s/%s/%s/%s' % \
                   (self.app, self.comp, self.cid,
                    mid, data_d['_time_right'])
        key_reduce = '%s/%s/%s/%s' % \
                   (self.app, self.comp, self.cid,
                    data_d['_time_left'])
        data_d['key_left'] = key_left
        data_d['key_right'] = key_right
        time_left = data_d['_time_left']
        time_sql = self._convert_time_str_to_sql(time_left)
        data_d['_time_sql'] = time_sql
        data_d['key_reduce'] = key_reduce
        data_d['ckp'] = str(ckp)
        return data_d


    def _convert_time_str_to_sql(self, time_left):
        time_sql = time.strftime('%Y-%m-%d %H:%M:%S',
                                        time.strptime(time_left,
                                                      '%Y%m%d_%H%M%S'))
        return time_sql


    def _update_ckp_history(self, data_d):
        """
        history is like:
        {'mysql:app_eemssec/company/9/35441/20170724_153000':
          {'kvarhttle': ['20170724_153000', Decimal('79.2')],
          'kvarhttli': ['20170724_153000', Decimal('271559.83425')],
          'kwhttle': ['20170724_153000', Decimal('9.6')],
          'kwhttli': ['20170724_153000', Decimal('1108220.09325')],
          'pttl': ['20170724_153000', Decimal('322.25')],
          'qttl': ['20170724_153000', Decimal('79.05')]}
        }
        """
        if not data_d:
            return ''
        key_left, key_right = data_d['key_left'], data_d['key_right']
        ckp_value = self._ckp_interpolate(data_d)
        if not key_left in self.history:
            # TODO: set left key if
            self.history[key_right] = ckp_value
            return self.history[key_right]
        else:
            ckp_value_old = self.history[key_left]
            ckp_value_new = self._inhere_history(ckp_value_old, ckp_value)
            self.history[key_right] = ckp_value_new
            return self.history[key_right]


    def _delete_old_history(self, data_d):
        """if there not exist new key, will not delete old key."""
        try:
            if not data_d:
                return None
            key_left, key_right = data_d['key_left'], data_d['key_right']
            if (key_left in self.history ) and (key_right in self.history):
                old_history = self.history.pop(key_left)
                return old_history
            else:
                return False
        except:
            return None


    def _ckp_interpolate(self, d):
        """
        input:
          {'data_left':{'20170106_121445':
          'kwhttli=12,kwhttle=1,pttli=2,pttle=3'},
          'data_right':{'20170106_121509':
          'kwhttli=18,kwhttle=1.3,pttli=4,pttle=3.2'}
          '(extral tags)':'',
          ...
          }

        output:
         {'kvarhttle': ['20170724_151500', Decimal('79.2')],
         'kvarhttli': ['20170724_151500', Decimal('271543.30625')],
         'kwhttle': ['20170724_151500', Decimal('9.6')],
         'kwhttli': ['20170724_151500', Decimal('1108154.46925')],
         'pttl': ['20170724_151500', Decimal('307.95')],
         'qttl': ['20170724_151500', Decimal('70.725')]
         }

        NOTICE:
          should check up all data, not only near ones.
        """
        if not (d['data_left']):
            return {}
        ks_left = sorted(d['data_left'].keys(), reverse=True)
        ks_right = sorted(d['data_right'].keys())
        rst = {}
        for i in self.variable_type:
            # value left
            have_value = False
            for k in ks_left:
                if have_value:
                    continue
                val = d['data_left'][k]
                time_int = int(time.mktime(
                    time.strptime(k, '%Y%m%d_%H%M%S')))
                vrs_value = self._vr_parse(val, i)
                if not None == vrs_value:
                    value_left = [time_int, vrs_value]
                    have_value = True
            if not have_value:
                value_left = None
            # value right
            have_value = False
            for k in ks_right:
                if have_value:
                    continue
                val = d['data_right'][k]
                time_int = int(time.mktime(
                    time.strptime(k, '%Y%m%d_%H%M%S')))
                vrs_value = self._vr_parse(val, i)
                if not None == vrs_value:
                    value_right = [time_int, vrs_value]
                    have_value = True
            if not have_value:
                value_right = None
            if (value_left is None and value_right is None):
                rst[i]=['00000000_000000', None]
                continue
            if not value_left:
                value_left = value_right
            if not value_right:
                value_right = value_left
                value_right[0] = value_right[0] + self.redis._interval - 1
            x0, y0 = self._iv(value_left + value_right)
            rst[i] = [time.strftime('%Y%m%d_%H%M%S', time.localtime(x0)),
                      y0]
        return rst


    def _vr_parse(self, sss, var_str):
        """
        input:
          ('var1=2,var3=2', 'var1')
        output:
          DFloat('2')
        """
        ss = sss.split(',')
        try:
            numbers = [DFloat(str(i.split('=')[1]))
                       for i in ss if str(var_str)+'=' in i]
            num = None if not numbers else numbers[0]
            return num
        except Exception as e:
            print("#19, ERROR:%s"%(repr(e)))
            return None


    def _iv(self, lst):
        """
        input:
          [time_left_int, value_left, time_right_int, value_right]
          (850, 5, 950, 10)
        output:
          DFloat ('7.5')
          Interpolate_Value at checkpoint, 900 in this case.
        NOTICE:
          if data around checkpoint is badpoint, should not
          create new badpoint with normal interpolate method.
        """
        x1, y1, x2, y2 = lst
        x1, y1, x2, y2 = DFloat(x1), DFloat(y1), DFloat(x2), DFloat(y2)
        x0 = x2 - x2 % self.redis._interval
        if (x1 is None or x2 is None or y1 is None or y2 is None):
            return [x0, None]
        if DFloat(str(x2 - x1)) == DFloat('0.0'):
            rt = [x1, DFloat(str(y1 + y2))/ DFloat('2.0')]
        else:
            ratio = DFloat(str(self.bad_point_threshold[1]))
            flag = ((y1 > DFloat('0.0')
                     and y2 > DFloat('0.0'))
                    and ((y2 / y1) > ratio
                         or (y2 / y1) < (1 / ratio)))
            if flag:
                rt = [x0, y1]
            else:
                rt = [x0, y1  + (y2 - y1) * (x0 - x1) / DFloat(str(x2 - x1))]
        # print("flag:%s, x1:%s, y1:%s, x2:%s, y2:%s, rt:%s"%(flag, x1, y1, x2, y2, rt))
        return rt


    def _inhere_history(self, old, new):
        """
        if new dictory is none, then use old one.
        """
        try:
            merged_dict = {}
            if old and (len(old) == len(new)):
                for k, v in new.iteritems():
                    merged[k] = v if v else old[k]
            else:
                merged_dict = new or old
            return merged_dict
        except:
            return new or old


    def sumup(self, data_d):
        """
        history is last round checkpoint value,
        it is in same shape of variable_types,
        with extral keys.

        input:
          (result of get_and_tag_data)
          {'data_left': {'20170714_110034': 'pttl=429.6,qttl=83.1',
            '20170714_111434': 'kwhttli=609217.5,kwhttle=562.5'},
          'data_right': {'20170714_111534': 'pttl=333.6,qttl=31.2',
            '20170714_111549': 'kwhttli=609225.0,kwhttle=562.5',
          '_time_left': '201707019_180000',
          '_time_right': '201707019_181500',
          '_time_sql': '2017-07-19 18:00:00',
          'key_left': 'mysql:app_eemsii/company/6/35334/201707019_180000',
          'key_right': 'mysql:app_eemsii/company/6/35334/201707019_181500'
          'key_reduce': 'mysql:app_eemsii/company/6/201707019_180000',
          }

        output:
          {'kvarhttle': Decimal('0.00'),
          'kvarhttli': Decimal('123.01'),
          'kwhttle': Decimal('0.00'),
          'kwhttli': Decimal('283.62'),
          'pttl': [Decimal('286.0540277778'), Decimal('0.0')],
          'qttl': [Decimal('123.7931944444'), Decimal('0.0')],
          '_time_left': '201707019_180000',
          '_time_right': '201707019_181500',
          '_time_sql': '2017-07-19 18:00:00',
          'key_left': 'mysql:app_eemsii/company/6/35334/201707019_180000',
          'key_right': 'mysql:app_eemsii/company/6/35334/201707019_181500'
          'key_reduce': 'mysql:app_eemsii/company/6/201707019_180000',
          'ckp': '900'
          }
        """
        if not data_d or not data_d['data_left']:
            return {}
        incr = {}
        for k in data_d:
            incr[k] = data_d[k]
        # TODO: should I use copy here?
        for var, type_value in self.variable_type.iteritems():
            if type_value == 0:
                incr[var] = self._delta(data_d, var)
            elif type_value == 1:
                incr[var] = self._intergrate(data_d, var, 'asis')
            elif type_value == 2:
                incr[var] = [self._intergrate(data_d, var, 'positive'),
                             self._intergrate(data_d, var, 'negative')]
        incr.pop('data_left')
        incr.pop('data_right')
        # original data is of no use.
        return incr


    def _delta(self, data_d, var):
        y1 = self.history[data_d['key_left']][var]
        y2 = self.history[data_d['key_right']][var]
        if (None == y1[1] or None == y2[1]):
            return None
        y1f = DFloat(str(y1[1]))
        y2f = DFloat(str(y2[1]))
        return ((y2f - y1f) + abs(y2f - y1f))/ DFloat('2.0')


    def _intergrate(self, data_d, var, s='positive'):
        """
        s options are: positive, negative, asis.
        """
        hst = self.history[data_d['key_left']]
        if None == hst[var][1]:
            return None
        ckp_value = self.history[data_d['key_right']]
        hst_one = DFloat(str(hst[var][1]))
        # hst[var][1] should never be None.
        ckp_one = DFloat(str(ckp_value[var][1]))
        v_left = data_d['data_left']
        if not v_left:
            return None
        left_keys = sorted(v_left.keys())
        int_v_left = [[self._int_f_k(k), self._vr_parse(v_left[k], var)]
                      for k in left_keys if self._vr_parse(v_left[k], var)]
        if not left_keys:
            int_v_left = [[self._int_f_k(data_d['_time_left']), None]]
        int_hst = [[self._int_f_k(data_d['_time_left']), hst_one]]
        int_ckp = [[self._int_f_k(data_d['_time_right']), ckp_one]]
        ti_value_all_raw = int_hst + int_v_left + int_ckp
        ti_value_all = self.filter_bad_point(ti_value_all_raw)
        v_sum = DFloat('0.0')
        def po(n):
            return (abs(n) + n) / DFloat('2.0')
        def ne(n):
            return (abs(n) - n) / DFloat('2.0')
        for i in zip(ti_value_all[:-1], ti_value_all[1:]):
            [x1, y1], [x2, y2] = i
            x1, y1 = DFloat(str(x1)), DFloat(str(y1))
            x2, y2 = DFloat(str(x2)), DFloat(str(y2))
            if  s=='negative':
                if (y1 is None or y2 is None):
                    acc = 0.0
                else:
                    acc = ne(y1 + y2)*(x2 - x1) / DFloat('2.0')
            elif s == 'positive':
                if (y1 is None or y2 is None):
                    acc = 0.0
                else:
                    acc = po(y1 + y2)*(x2 - x1) / DFloat('2.0')
            else:
                if (y1 is None or y2 is None):
                    acc = 0.0
                else:
                    acc = (y1 + y2)*(x2 - x1) / DFloat('2.0')
            v_sum += acc
        return v_sum/DFloat(str(self.seconds_one_hour))


    def _int_f_k(self, k):
        return int(time.mktime(time.strptime(k, '%Y%m%d_%H%M%S')))


    def filter_bad_point(self, lst):
        try:
            limit, ratio, have_points = self.bad_point_threshold
            int_s = [i[0] for i in lst]
            val_s = [i[1] for i in lst]
            val_without_max = [i for i in val_s if not i == max(val_s)]
            if not val_without_max:
                return lst
            if ((len(val_s) - len(val_without_max) <= limit) and
                (len(val_s) > have_points) and
                (DFloat(str(max(val_s))) / DFloat(str(max(val_without_max))) > ratio)):
                lst_filted = [i for i in lst if not i[1] == max(val_s)]
                print("filtering, max:%s, second max:%s"%(max(val_s), max(val_without_max)))
                return lst_filted
            else:
                return lst
        except Exception as e:
            print("#26, ERROR:%s"%(repr(e)))
            return lst


    def _company_addup(self, sql_recd_d_ori, sumup_d):
        """
        input:
          sql_recd_d_ori: {}
          sumup_d:
          {'key_reduce':'mysql:app_eemsii/company/6/201707019_180000',
            '_time_sql': '2017-07-19 18:00:00',
            'charge': 62.03528756860898,
            'kvarhi': -17.749406617955692,
            'kwhe': 0.0,
            'kwhi': 166.26986751168315,
            'p': 2660.3178801869303,
            'q': 477.80727717022285,
            'spfv': 'v'
          }
        output:
          {'mysql:app_eemsii/company/6/201707019_180000':
            {'_time_sql': '2017-07-19 18:00:00',
              'charge': 62.03528756860898,
              'kvarhi': -17.749406617955692,
              'kwhe': 0.0,
              'kwhi': 166.26986751168315,
              'p': 2660.3178801869303,
              'q': 477.80727717022285,
              'spfv': 'v'
            }
          }
        NOTICE:
          this seems not a normal function,
          because it is not the return value used.
        """
        sql_recd_d = copy.deepcopy(sql_recd_d_ori)
        new_sql_recd = self.apply_price(sumup_d)
        if not 'key_reduce' in new_sql_recd:
            return sql_recd_d
        key_reduce = new_sql_recd.pop('key_reduce')
        # change dictory structure.
        if key_reduce not in sql_recd_d:
            sql_recd_d[key_reduce] = new_sql_recd
        else:
            exist_recd = sql_recd_d[key_reduce]
            sql_recd_d[key_reduce] = self._add_up_dict(exist_recd, new_sql_recd)
        return sql_recd_d


    def apply_price(self, sumup_d):
        """
        convert kwhttli and pttl variables
        into prince infomation, using price_d
        and meter_control_police_dic.
        input:
          {'kvarhttle': Decimal('0.00'),
          'kvarhttli': Decimal('123.01'),
          'kwhttle': Decimal('0.00'),
          'kwhttli': Decimal('283.62'),
          'pttl': [Decimal('286.0540277778'), Decimal('0.0')],
          'qttl': [Decimal('123.7931944444'), Decimal('0.0')],
          '_time_left': '20170719_180000',
          '_time_right': '20170719_181500',
          '_time_sql': '2017-01-18 06:30:00',
          'key_left': 'mysql:app_eemsii/company/6/35334/201707019_180000',
          'key_right': 'mysql:app_eemsii/company/6/35334/201707019_181500'}
          'key_reduce': 'mysql:app_eemsii/company/6/201707019_180000',
        output:
          {'_time_sql': '2017-07-19 18:00:00',
          'key_reduce': 'mysql:app_eemsii/company/6/201707019_180000',
          'charge': 62.03528756860898,
          'kvarhi': -17.749406617955692,
          'kwhe': 0.0,
          'kwhi': 166.26986751168315,
          'p': 2660.3178801869303,
          'q': 477.80727717022285,
          'spfv': 'v'},
        """
        if not sumup_d:
            return {}
        try:
            rlt_d = {}
            app, comp, cid, meter_id, time_left = sumup_d['key_left'].split('/')
            key_reduce = sumup_d['key_reduce']
            ckp = sumup_d['ckp']
            pli_d = self.sql.get_meter(comp, cid, ckp)[meter_id]
            price_d = self.sql.get_price(comp, cid, ckp)
            _time_sql = sumup_d['_time_sql']
            rlt_d['_time_sql'] = _time_sql
        except Exception as e:
            print("#22, ERROR:%s"%(repr(e)))
            return rlt_d
        if not price_d:
            print("#8, price_d %s, please check mysql config"%(price_d))
            return {}
        if not ('use_energy' in pli_d and 'use_power' in pli_d):
            print("use_energy or use_power not in pli_d:%s, \
            please check mysql config"%(pli_d))
            return {}
        rlt_d['spfv'] = price_d['hours'][int(sumup_d['_time_left'][9:11])]
        rate = price_d[rlt_d['spfv']]
        trans_factor = DFloat(str(self.seconds_one_hour / self.redis._interval))
        rlt_d['key_reduce'] = key_reduce
        rlt_d['_use_energy'] = pli_d['use_energy']
        if ((pli_d['use_energy'] == '0')
           or (pli_d['use_energy'] == '4')
           or (pli_d['use_energy'] == '8')):
            rlt_d['kwhi'] = sumup_d['pttl'][0]
            rlt_d['kwhe'] = sumup_d['pttl'][1]
        else:
            rlt_d['kwhi'] = sumup_d['kwhttli']
            rlt_d['kwhe'] = sumup_d['kwhttle']
        if ((pli_d['use_energy'] == '0')
           or (pli_d['use_energy'] == '4')
           or (pli_d['use_energy'] == '5')):
            rlt_d['kvarhi'] = sumup_d['qttl'][0]
            rlt_d['kvarhe'] = sumup_d['qttl'][1]
        else:
            rlt_d['kvarhi'] = sumup_d['kvarhttli']
            rlt_d['kvarhe'] = sumup_d['kvarhttle']
        rlt_d['_use_power'] = pli_d['use_power']
        if ((pli_d['use_power'] == '0')
            or (pli_d['use_power'] == '4')
            or (pli_d['use_power'] == '8')):
            if (sumup_d['kwhttli'] is None):
                rlt_d['p'] = None
            else:
                rlt_d['p'] = sumup_d['kwhttli'] * trans_factor
            if (sumup_d['kwhttli'] is None):
                rlt_d['pi'] = None
            else:
                rlt_d['pi'] = sumup_d['kwhttli'] * trans_factor
            if (sumup_d['kwhttle'] is None):
                rlt_d['pe'] = None
            else:
                rlt_d['pe'] = sumup_d['kwhttle'] * trans_factor
        else:
            if not (sumup_d['pttl'][0] is None
                    or sumup_d['pttl'][1] is None):
                rlt_d['p'] = ((sumup_d['pttl'][0]
                               - sumup_d['pttl'][1])
                              * trans_factor)
            else:
                rlt_d['p'] = None
            if not (sumup_d['qttl'][0] is None
                    or sumup_d['qttl'][1] is None):
                rlt_d['q'] = ((sumup_d['qttl'][0]
                               - sumup_d['qttl'][1])
                              * trans_factor)
            else:
                rlt_d['q'] = None
            if (sumup_d['pttl'][0] is None):
                rlt_d['pi'] = None
            else:
                rlt_d['pi'] = sumup_d['pttl'][0] * trans_factor
        if ((pli_d['use_power'] == '0')
            or (pli_d['use_power'] == '4')
            or (pli_d['use_power'] == '5')):
            if (sumup_d['kvarhttli'] is None):
                rlt_d['q'] = None
            else:
                rlt_d['q'] = sumup_d['kvarhttli'] * trans_factor
            if (sumup_d['kvarhttli'] is None):
                rlt_d['qi'] = None
            else:
                rlt_d['qi'] = sumup_d['kvarhttli'] * trans_factor
            if (sumup_d['kvarhttle'] is None) :
                rlt_d['qe'] = None
            else:
                rlt_d['qe'] = sumup_d['kvarhttle'] * trans_factor
        else:
            rlt_d['_use_power'] = pli_d['use_power']
            if (sumup_d['pttl'][1] is None):
                rlt_d['pe'] = None
            else:
                rlt_d['pe'] = sumup_d['pttl'][1] * trans_factor
            if (sumup_d['qttl'][0] is None):
                rlt_d['qi'] = None
            else:
                rlt_d['qi'] = sumup_d['qttl'][0] * trans_factor
            if (sumup_d['qttl'][1] is None):
                rlt_d['qe'] = None
            else:
                rlt_d['qe'] = sumup_d['qttl'][1] * trans_factor
        if not rlt_d['kwhi'] is None:
            rlt_d['charge'] = rate * rlt_d['kwhi']
        else:
            rlt_d['charge'] = None
        return rlt_d


    def _add_up_dict(self, d1_ori, d2_ori):
        d1 = copy.deepcopy(d1_ori)
        d2 = copy.deepcopy(d2_ori)
        for k1, v1 in d1.items():
            v2 = d2[k1] if k1 in d2 else None
            if type(v1) == str or type(v1) == unicode:
                v_add = v1
            else:
                v_add = v1 + v2 if (v1 and v2) else (v1 or v2)
            d1[k1] = v_add
        return d1 or d2


    def process(self):
        time_a = time.time()
        self.redis.refresh_start_time(time.time())
        comp_list = []
        for comp_cid, workshop_d in self.sql.map_d[self.app].iteritems():
            for workshop in workshop_d:
                comp_list.append(workshop)
            comp_list.append(comp_cid)
        calc_rlt = map(self.one_comp_calc, comp_list)
        sql_rlt  = map(self.sql.sql_op, calc_rlt)
        time_b = time.time()
        print("SPEND TIME %s s"%(int(time_b - time_a)))
        return calc_rlt


def app_calc_loop(app, chosed_company=[]):
    s = AppCalc(app)
    s.ckps = s.ckps_loop
    s.sql.chose_company(chosed_company)
    print("run first task")
    rlt = s.process()
    print("Result Is:")
    for d in rlt:
        for k in d:
            print("-->  %s:\n%s"%(k, d[k]))
    s.set_prepare_task_flag()
    # init prepare task again.
    while True:
        try:
            how_about_sleep()
            rlt = s.process()
            print("Result Is:")
            for d in rlt:
                for k in d:
                    print("-->  %s:\n%s"%(k, d[k]))
            print("refresh mysql meta info.")
            s.sql.get_meta_d()
            s.sql.get_map_d()
            s.sql.chose_company(chosed_company)
            s.sql.clear_mysql_pool()
        except Exception as e:
            if have_hzlog:
                hlog.error("#0 ERROR:%s"%(repr(e)))
            else:
                print("#0 ERROR:%s"%(repr(e)))


def how_about_sleep(shift=int(5),
                    interval=INTERVAL,
                    real_sleep=True):
    sleep_for_i = interval - (int(time.time() - int(shift)) % interval)
    sleep_for = (sleep_for_i + abs(sleep_for_i)) / 2
    print('sleeping for %s' % (sleep_for))
    end_time_str = time.strftime('%Y-%m-%d %H:%M:%S',
                                 time.localtime(time.time() + sleep_for))
    print(end_time_str)
    if real_sleep:
        time.sleep(sleep_for)
    return sleep_for


def app_calc_once(app,
                  ckps,
                  chosed_company=[],
                  nocp=False,
                  zdxl=False):
    s = AppCalc(app)
    if zdxl:
        s.redis._interval = 60
        print("interval is:%s"%(s.redis._interval))
    s.un_set_prepare_task_flag()
    s.ckps = sorted(ckps, reverse=True)
    s.sql.chose_company(chosed_company)
    s.sql.nocp = nocp
    rlt = s.process()
    print("RESULT IS:\n")
    for d in rlt:
        for k in d:
            print("-->  %s:\n%s"%(k, d[k]))
    return rlt


def print_help():
    help_message = """
    Get company/workshop/meter meta info from mysql database,
    meter data from redis,
    calculate charge and kwhi, p &etc of company/workshop,
    write record into mysql data base.

    Usage:

        REDO MODE:
        --redo    redo mode
        --ymd     followed by year/mounth/day/hour/minute/second
                  to start with. Default begining of today.
        --length  followed by how long do you want, in second or
                  special string. Default 86400(1d).
                  Y=year, M=mounth, d=day, h=hour, m=minute, s=second,
                  such as 1d3h for one day and three hour.
        --app     app name, such as eemssec, eemsyd.
        --company companys to process. optional,
                  if not given. use all company.
        --nocp    caculate and show result, not write mysql database.
        --zdxl    set INTERVAL to 60s, for zuidaxuliang calculation.


        LOOP MODE:
        --app     app name, such as eemssec, eemsyd.

    Example:

        python app_calc_v2.py  --redo --ymd 2017/07/26/01/00/00
            --length 3h --app eemssec --company 5 6   --nocp

        python app_calc_v2.py --app eemssec
    """
    print(help_message)
    return True


def main(argv_d = {}):
    if not argv_d:
        argv_d = tools.parse_argv.parse_argv()
    if ('help' in argv_d) or ('h' in argv_d):
        return print_help()
    if 'company' in argv_d:
        wanted_company_list = tools.parse_argv.parse_range(argv_d['company'])
    else:
        wanted_company_list = []
    if 'redo' in argv_d or 'ymd' in argv_d:
        if not ('ymd' in argv_d and 'length' in argv_d and 'app' in argv_d):
            print("please set --ymd, --length and --app")
            return False
        app_redo = "mysql:app_%s"%(argv_d['app'])
        int_start = int(time.time() -
                        time.mktime(time.strptime(argv_d['ymd'],
                                                  '%Y/%m/%d/%H/%M/%S')))
        ckp_start = int(int_start - int_start % 900)
        argv_length = tools.parse_argv.parse_time_length(argv_d['length'])
        if argv_length == None:
            print('Wrong length is given.')
            return False
        ckps_redo = [ckp_start - int(i) * 900
                     for i in range(argv_length / 900 + 1)]
        nocp = True if 'nocp' in argv_d else False
        zdxl = True if 'zdxl' in argv_d else False
        rlt = app_calc_once(app_redo, ckps_redo, wanted_company_list, nocp, zdxl)
    else:
        app_loop = "mysql:app_%s"%(argv_d['app'])
        app_calc_loop(app_loop, wanted_company_list)


if __name__ == '__main__':
    main()
