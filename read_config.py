# -*- coding: utf-8 -*-
#!/usr/bin/env python

# Author: chenjinqian
# Email: 2012chenjinqian@gmail.com

"""
read config file in style of agcavc project, which means
a/b=c
with be readed as {'a':{'b':'c'}}, dictory can be nested.
Usage:
cfg = ReadConfig(path),
cfg_dic=cfg.check_config() to get a refreshed config,
cfg.config to get the cached config dic.
"""

__all__ = ['ReadConfig']


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



def main():
    # TODO: If args is -h, then print help message.
    print("tool class for reading config")
    print("Usage: cfg = ReadConfig(path), cfg_dic=cfg.check_config() to get a refreshed config, and cfg.config to get the cached config dic.")
    print("reading config file db.info... ")
    import os
    fp = os.path.realpath('db.info')
    config=ReadConfig(fp)
    # config_dic = config.check_config()
    config_dic = config.config
    print("config_dic is %s" % config_dic)


if __name__ == '__main__':
    main()
