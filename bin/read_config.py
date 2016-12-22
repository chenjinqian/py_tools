# -*- coding: utf-8 -*-
#!/usr/bin/env python

# Author: chenjinqian
# Email: 2012chenjinqian@gmail.com

"""
1. AGCAVC style config file
a/b=c
will be readed as {'a':{'b':'c'}}, dictory can be nested.

Usage:
from read_config import ReadConfig as ReadConfig
cfg = ReadConfig(path) # config instance
cfg.config # get the cached config dic.
cfg_dic=cfg.check_config() # get a refreshed config,

2. DB type config file is like:
[mysql:db_name]
ip=xx.xx.xx.xx
port = xx

use ReadConfig_DB will read this file as dictory{'mysql:db_name':{'ip':'xx.xx.xx.xx', port:xx}}

Usage:
cfg2 = ReadConfig_DB(path)        # config instance
config_dict = cfg2.config         # config dictory
config_dict = cfg2.check_config() # refresh config dictory
"""

import re, os, sys

__all__ = ['ReadConfig', 'ReadConfig_DB']


class ReadConfig(object):
    def __init__(self, fpath):
        "docstring"
        self.fpath=str(fpath)
        self.config={}
        self.check_config()

    def check_config(self):
        config = {}
        try:
            with open(self.fpath, 'rb') as f:
                self.data = f.readlines()
        except:
            self.config = {}
            return self.config
        for d in self.data:
            try:
                d = d.replace('\40', '')
                d = d.replace('\r', '')
                d = d.replace('\n', '')
                if d=='' or d[0] == '#':
                    continue
                d = d.split('=')
                if len(d) == 2:
                    d[0] = d[0].split('/')
                    tmp = config
                    for i in range(0, len(d[0])):
                        if not d[0][i] in tmp:
                            if i == len(d[0]) - 1:
                                tmp[d[0][i]] = d[1]
                            else:
                                tmp[d[0][i]] = {}
                        tmp = tmp[d[0][i]]
            except:
                continue
        self.config = config
        return config


class ReadConfig_DB(object):
    def __init__(self, fpath):
        "docstring"
        self.fpath = str(fpath)
        self.check_config(set_config=True)

    def db_style_convert(self, key_str):
        """if key is not"""
        if self.db_type == 'mysql':
            self.rpl += ['username', 'user', 'password', 'passwd', 'database', 'db', 'dbname', 'db']
        if self.rpl:
           for rp in zip(*[iter(self.rpl)]*2):
               # [1,2,3,4] to [[1,2], [3,4]]
               try:
                   if key_str == rp[0]:
                       # print('cached %s' % key_str)
                       return str(rp[1])
               except Exception:
                   continue
           return key_str
        else:
            return key_str


    def check_config(self, db_type='', convert_port=False, set_config=False, replace=[]):
        self.convert_port = convert_port
        self.db_type=db_type
        self.rpl = replace
        # print('replace is %s' % replace)
        try:
            with open(self.fpath, 'rb') as f:
                self.data = f.readlines()
        except:
            self.config = {}
            return self.config
        zone_key = ''
        config = {}
        for d in self.data:
            # print(d)
            d = d.replace('\40', '')
            d = d.replace('\r', '')
            d = d.replace('\n', '')
            if d=='' or d[0] == '#':
                continue
            re_m = re.match('^\[(.*)\]$', d)
            # print(d, re_m)
            if re_m:
                zone_key= re_m.group(1)
                config[zone_key] = {}
                # print(zone_key)
            elif '=' in d:
                d = d.split('=')
                if not len(d) == 2:
                    continue
                if zone_key:
                    if self.convert_port and d[0] in ['port', 'Port', 'PORT']:
                        try:
                            config[zone_key][self.db_style_convert(d[0])] = int(d[1])
                        except:
                            print('fail in converting port to int')
                            return -1
                            # if fail to convert, just crash print.
                    else:
                        config[zone_key][self.db_style_convert(d[0])] = d[1]
            else:
                continue
        if set_config:
            self.config = config
        return config


# class ReadConfig_DB_v01(object):
#     def __init__(self, fpath):
#         "docstring"
#         self.fpath = str(fpath)
#         self.check_config()

#     def db_style_convert(self, key_str):
#         """if key is not"""
#         if self.db_type=='mysql':
#             lower_str = str(key_str).lower()
#             if lower_str in ['username', 'user', 'name']:
#                 return 'user'
#             if lower_str in ['password', 'pwd']:
#                 return 'passwd'
#             if lower_str in ['database', 'db_name', 'dbname']:
#                 return 'db'
#             else:
#                 return key_str
#         else:
#             return key_str


#     def check_config(self, db_type='', convert_port=False):
#         self.convert_port = convert_port
#         self.db_type=db_type
#         try:
#             with open(self.fpath, 'rb') as f:
#                 self.data = f.readlines()
#         except:
#             self.config = {}
#             return self.config
#         zone_key = ''
#         config = {}
#         for d in self.data:
#             # print(d)
#             d = d.replace('\40', '')
#             d = d.replace('\r', '')
#             d = d.replace('\n', '')
#             if d=='' or d[0] == '#':
#                 continue
#             re_m = re.match('^\[(.*)\]$', d)
#             # print(d, re_m)
#             if re_m:
#                 zone_key= re_m.group(1)
#                 config[zone_key] = {}
#                 # print(zone_key)
#             elif '=' in d:
#                 d = d.split('=')
#                 if not len(d) == 2:
#                     continue
#                 if zone_key:
#                     if self.convert_port and d[0] in ['port', 'Port', 'PORT']:
#                         try:
#                             config[zone_key][self.db_style_convert(d[0])] = int(d[1])
#                         except:
#                             print('fail in converting port to int')
#                             return -1
#                             # if fail to convert, just crash print.
#                     else:
#                         config[zone_key][self.db_style_convert(d[0])] = d[1]
#             else:
#                 continue
#         self.config = config
#         return self.config


def main():
    # TODO: If args is -h, then print help message.
    print("tool class for reading config")
    print("Usage: cfg = ReadConfig(path), cfg_dic=cfg.check_config() to get a refreshed config, and cfg.config to get the cached config dic.")
    print("reading config file db.info... ")
    import os
    fp = os.path.realpath('../config/db.info')
    config=ReadConfig(fp)
    # config_dic = config.check_config()
    config_dic = config.config
    print("config_dic is %s" % (config_dic))
    fp2 = os.path.realpath('../config/db.ini')
    config2 = ReadConfig_DB(fp2)
    config_dic2 = config2.config
    config_dic2_checkconfig = config2.check_config()
    print("config_dic2 is %s" % (config_dic2))


if __name__ == '__main__':
    main()
