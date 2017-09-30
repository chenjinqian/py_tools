#!/usr/bin/python
#-*- coding:utf-8 -*-

## author: JinQian.Chen
## usage example:  python3  file_process.py
# all mission in once.
# TODO: multiply micro-process uploading. not every device support gevent.

import os, sys, re, datetime
# import glob
import time
# import threading, time, Queue    # TODO: use mutiply thread.
import paramiko
# TODO:
import ftplib
import log
import findfiles

default_user = 'guest'
default_passwd = '123456'

# this is default config, but will prefer cfg.py first.
config = {
    'mission1':{
        'ip':'172.18.105.29',
        'port':22,
        'protocol':'sftp',
        'type':'download',
        'local_dir':'/eppds/ftproot',
        'remote_dir':'/eppds/toupload/predictor',
        'file':'^nwp_\d\d\d\d\d\d\d\d.txt$',
        'user':'guest',
        'pkey':'/home/lcg/.ssh/id_eppds_user',
        'passwd': '123456',
        'del_file': False,
        'order':'old/new/mix/desc/asc',
        'limit': 50,
        'fast': False,
                  },
    'mission2':{
        'ip':'172.18.105.29',
        'port':22,
        'protocol':'sftp',
        'type':'upload',
        'local_dir':'/eppds/toupload/predictor',
        'remote_dir':'/eppds/ftproot',
        'file':'^nwp_\d\d\d\d\d\d\d\d.txt$',
        'user':'guest',
        'passwd': '123456',
        'del_file':True
    },
    'mission3':{
        'ip':'172.18.105.29',
        'port':21,
        'protocol':'ftp',
        'type':'download',
        'local_dir':'/eppds/ftproot',
        'remote_dir':'/',
        'file':'^nwp_\d\d\d\d\d\d\d\d.txt$',
        'user':'guest',
        'passwd': '123456',
    },
    'mission4':{
        'ip':'172.18.105.29',
        'port':21,
        'protocol':'ftp',
        'type':'upload',
        'local_dir':'/eppds/toupload/predictor',
        'remote_dir':'/',
        'file':'^nwp_\d\d\d\d\d\d\d\d.txt$',
        'user':'guest',
        'passwd': '123456',
    },
}

# try to use config file first
default_del_file = True

# try:
#     try:
#         os.unlink('./cfg.pyc')
#     except:
#         pass
#     import cfg as cfgpy
#     print('success import cfg')
#     cfg = cfgpy.config
#     sleepfor = cfgpy.sleepfor
# except:
#     cfg = config
#     print("fail to import cfg %s" % str(sys.exc_info()))
#     sleepfor = 10

# cfg = config

class Server(object):
    # set up parameters.
    def __init__(self, dic):
        for k, v in dic.items():
            setattr(self, k, v)
        self.regex = re.compile(self.file)
        self.connected = False
        self.dir_checked = False
        if not hasattr(self, 'user'):
            self.user = default_user
        if not hasattr(self, 'passwd'):
            self.passwd = default_passwd
        if not hasattr(self, 'del_file'):
            self.del_file = default_del_file
        self.del_file = self._convert_to_bool(self.del_file)
        if not hasattr(self, 'order'):
            self.order = 'mix'
        if not hasattr(self, 'limit'):
            self.limit = '-1'
        if not hasattr(self, 'pkey'):
            self.pkey = ''
        if not hasattr(self, 'timeout'):
            self.timeout = 60
        if not hasattr(self, 'recursive'):
            self.recursive = False
        if not hasattr(self, 'fast'):
            self.fast = False
        if not hasattr(self, 'maxfail'):
            self.maxfail = 5
        if not hasattr(self, 'refresh'):
            self.refresh = True
        if not hasattr(self, 'bak'):
            self.bak = ''
        self.refresh = self._convert_to_bool(self.refresh)
        self.connect()
        self.set_up_trans_method()


    def _convert_to_bool(self, s):
        if (('False' == s) or
            ('false' == s) or
            ('F' == s) or
            ('f' == s) or
            ('0' == s) or
            ('None' == s) or
            ('none' == s) or
            ('no' == s) or
            ('N' == s) or
            ('n' == s)):
            return False
        else:
            return True


    def connect(self):
        self.close()
        if self.protocol == 'sftp':
            try:
                # print('start sftp connection. ')
                if self.pkey:
                    # TODO: Not working well using private keys.
                    c = paramiko.SSHClient()
                    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    c.connect(hostname=self.ip, port=self.port,
                              key_filename=self.pkey, username=self.user)
                    self.sftp = c.open_sftp()
                else:
                    # self.transport = paramiko.Transport((self.ip, int(self.port)))
                    # self.transport.connect(username=self.user, password=self.passwd)
                    # self.sftp = paramiko.SFTPClient.from_transport(self.transport)
                    self.ssh = paramiko.SSHClient()
                    self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    self.ssh.connect(hostname=self.ip,
                                     port=self.port,
                                     username=self.user,
                                     password=self.passwd,
                                     timeout=int(self.timeout))
                    self.sftp = self.ssh.open_sftp()
                    # TODO: self.timeout
                # print('connection established')
                self.connected = True
                log.log("connect success.")
                return True
            except Exception as e:
                print("#11, ERROR:%s"%(repr(e)))
                self.close()
                # print ("#3, Unexpected error:", sys.exc_info())
                log.log('#51, sftp connection fail,%s'%(str(sys.exc_info())))
                self.connected = False
                return False
        elif self.protocol == 'ftp':
            try:
                # print('start ftp connection. ')
                # self.ftp.connect(self.ip, int(self.port))
                self.ftp = ftplib.FTP(timeout=self.timeout)
                self.ftp.connect(self.ip, int(self.port))
                # self.ftp = ftplib.FTP(self.ip)
                # print self.ftp.getwelcome()
                self.ftp.login(self.user, self.passwd)
                # self.ftp.cwd(self.remote_dir)
                # print('ftp connection successed')
                self.connected = True
                log.log("connect success.")
                return True
            except:
                self.close()
                log.log('ftp connection fail,%s'%(str(sys.exc_info())))
                self.connected = False
                return False
        elif self.protocol == 'mv' or self.protocol == 'local':
            return True


    def set_up_trans_method(self):
        # print(self.type, self.protocol, 'starting')
        # del_file = self.del_file
        # TODO: Notice, here variable not passing downside to sub-def method.
        if self.protocol == 'sftp':
            if self.type == 'upload':
                self.tran_method = self.sftp_upload
                if self.fast:
                    self.tran_method = self.sftp_upload_fast
                return True
            elif self.type == 'download':
                self.tran_method = self.sftp_download
                return True
            else:
                return False
        if self.protocol == 'ftp':
            if self.type == 'upload':
                self.tran_method = self.ftp_upload
                # print(self.type, self.protocol, 'pass')
                return True
            elif self.type == 'download' and self.protocol == 'ftp':
                self.tran_method = self.ftp_download
                # print(self.type, self.protocol, 'pass')
                return True
            else:
                return False
        return False


    def close(self):
        # print('Closing the connection')
        try:
            self.connected = False
            if 'sftp' == self.protocol:
                # if self.transport.is_active():
                #     self.transport.close()
                try:
                    if hasattr(self, 'sftp'):
                        self.sftp.close()
                        del(self.sftp)
                except Exception as e:
                    log.log("#91, can not close self.sftp, error: %s"%(str(repr(e))))
                    # print("#91, can not close self.sftp, error: %s"%(str(repr(e))))
                    # # Notice, only close transport will terminate subprocess.
                    pass
                if hasattr(self, 'transport'):
                    self.transport.close()
                    del(self.transport)
                return True
            elif 'ftp' == self.protocol:
                if hasattr(self, 'ftp'):
                    self.ftp.close()
                    del(self.ftp)
                return True
        except Exception as e:
            log.log("#92, error in colse connection: %s"%(str(repr(e))))
            print("#92, error in colse connection: %s"%(str(repr(e))))
            return False


    def sftp_download(self, from_file, to_file):
        tmp_file = os.path.join(to_file + u'.tmpdown')
        self.sftp.get(from_file, tmp_file)
        size_remote = self.sftp.stat(from_file).st_size
        size_local = os.stat(tmp_file).st_size
        if size_remote == size_local:
            # print('rename the file')
            os.rename(tmp_file, to_file)
            log.log("sftp download file from %s to %s, delete remote: %s" % (from_file, to_file, self.del_file))
            if self.del_file:
                # print('deleting', from_file)
                self.sftp.unlink(from_file)
            return True
        return False


    def sftp_upload(self, from_file, to_file):
        tmp_file = os.path.join(to_file + u'.tmpup')
        try:
            if self.refresh:
                self.sftp.unlink(to_file)
                self.sftp.unlink(tmp_file)
                log.log("deleted dumplicate tmp_file:%s, to_file:%s"%(to_file, tmp_file))
        except Exception as e:
            # log.log("#88, %s"%(str(repr(e))))
            pass
        self.sftp.put(from_file, tmp_file)
        size_remote = self.sftp.stat(tmp_file).st_size
        size_local = os.stat(from_file).st_size
        # print('remote file size %s and local file size %s' % (size_remote, size_local))
        if size_remote == size_local:
            # print('rename the file')
            # time.sleep(5)
            self.sftp.rename(tmp_file, to_file)
            log.log("sftp upload file from %s to %s, delete: %s" % (from_file, to_file, self.del_file))
            if self.del_file:
                # print('deleting', from_file)
                if self.bak:
                    os.system("mv %s %s"%(from_file, self.bak))
                else:
                    os.unlink(from_file)
            return True
        return False


    def sftp_upload_fast(self, from_file, to_file):
        """not practical, it's not suggested to use."""
        self.sftp.put(from_file, to_file)
        if self.del_file:
            os.unlink(from_file)
        return False


    def ftp_download(self, from_file, to_file):
        tmp_file = os.path.join(to_file + u'.tmpdown')
        with open(tmp_file, 'wb') as f:
            self.ftp.retrbinary('RETR %s' % from_file, f.write)
            # f.close()
        try:
            size_remote = self.ftp.size(from_file)
            size_local = os.stat(tmp_file).st_size
            # print('remote file size %s and local file size %s' % (size_remote, size_local))
            if size_remote == size_local:
                os.rename(tmp_file, to_file)
                log.log("ftp download file from %s to %s, delete remote: %s" % (from_file, to_file, self.del_file))
                if self.del_file:
                    self.ftp.delete(from_file)
                return True
        except:
            log.log("#72, delet error:%s"%(str(sys.exc_info())))
            return False
        return False


    def ftp_upload(self, from_file, to_file):
        tmp_file = os.path.join(to_file + u'.tmpup')
        file_handler = open(from_file, 'rb')
        self.ftp.storbinary('STOR ' + tmp_file, file_handler, 1024)
        file_handler.close()
        try:
            size_remote = self.ftp.size(tmp_file)
            size_local = os.stat(from_file).st_size
            # print('remote file size %s and local file size %s' % (size_remote, size_local))
            if self.del_file and size_remote == size_local:
                # print('rename the file')
                # time.sleep(5)
                self.ftp.rename(tmp_file, to_file)
                log.log("ftp upload file from %s to %s, delete: %s" % (from_file, to_file, self.del_file))
                if self.del_file:
                    # print('deleting', from_file)
                    if self.bak:
                        os.system("mv %s %s"%(from_file, self.bak))
                    else:
                        os.unlink(from_file)
                return True
        except:
            log.log("#73, delete error:%s"%(str(sys.exc_info())))
            return False


    def info(self):
        print('type: ', self.type, 'protocol: ', self.protocol, 'port: ', self.port, 'local_dir: ', self.local_dir, 'remote_dir: ', self.remote_dir)
        # init connect.


    def ensure_remote_dir_exists(self):
        if self.protocol == 'sftp':
            try:
                self.sftp.chdir(self.remote_dir)  # Test if remote_path exists
                self.connected = True
                self.dir_checked = True
                return True
            except:
                try:
                    self.sftp.mkdir(self.remote_dir)  # Create remote_path
                    self.sftp.chdir(self.remote_dir)
                    # print(self.sftp.chdir(self.remote_dir))
                    self.connected = True
                    self.dir_checked = True
                    return True
                except:
                    self.connected = False
                    self.dir_checked = False
                    log.log('dir check have failed. %s' % (self.remote_dir))
                    return False
        elif self.protocol == 'ftp':
            try:
                self.ftp.cwd(self.remote_dir)
                self.connected = True
                self.dir_checked = True
                return True
            except:
                try:
                    self.ftp.mkd(self.remote_dir)
                    self.connected = True
                    self.dir_checked = True
                    return True
                except:
                    log.log('fail to create remote ftp dires')
                    self.connected = False
                    self.dir_checked = False
                    return False
        elif self.protocol == 'mv' or self.protocol == 'local':
            have_remote_dir = os.path.isdir(self.remote_dir)
            have_locale_dir =  os.path.isdir(self.local_dir)
            if have_remote_dir and have_locale_dir:
                return True
            else:
                try:
                    if not have_remote_dir: os.mkdir(self.remote_dir)
                    if not have_locale_dir: os.mkdir(self.local_dir)
                    return True
                except:
                    return False
            # TODO: should check glob wildcard match, if only one match, also return True.


    def process_file(self, files_to_process):
        if self.protocol == 'local' or self.protocol == 'mv':
            try:
                for item in files_to_process:
                    os.system('mv %s %s' % (item[0], item[1]))
                    log.log('mv %s %s' % (item[0], item[1]))
                return True
            except:
                return False
        # Then, protocol should be sftp or ftp.
        # make sure the remote folder exist, and cd to that folder.
        if not self.connected:
            rety = self.connect()
            # print('try connect: ', rety)
            if not rety:
                # print('can not connect. ')
                return False
        if not self.dir_checked:
            rety = self.ensure_remote_dir_exists()
            if not rety:
                return False
        if not files_to_process:
            log.log('get empty list,%s,%s'%(self.type, self.protocol))
            return False
        cnt = len(files_to_process)
        fail_in_row = 0
        for item in files_to_process:
            try:
                time.sleep(0.003)
                put_p = self.tran_method(item[0], item[1])
                cnt = cnt - 1
                fail_in_row = 0
            except Exception as e:
                self.ensure_remote_dir_exists()
                fail_in_row += 1
                print("#70, trans error:%s" %(str(sys.exc_info())))
                print("%s"%(e))
                log.log("#70, trans error:%s" %(str(sys.exc_info())))
                log.log("#71, processing item: %s"%(item))
                if fail_in_row > self.maxfail:
                    log.log("#75, fail in row more than 5 times, abandon rest %s files" % (cnt))
                    print("#75, fail in row more than 5 times, abandon rest %s files" % (cnt))
                    return False
                if self.connected:
                    log.log("#73, no need to reconnect. pass this item, continue")
                    continue
                else:
                    for i in range(3):
                        if not self.connected:
                            time.sleep(3 + i * 10)
                            self.connect()
                if not self.connected:
                    log.log("#72, fail to re-connect in. breaking file processing, abandon rest %s files"%(cnt))
                    return False
        return True


    def files_in_dir(self):
        """if not connected, will not check files."""
        # self.info()
        # if (not self.connected) and self.type == 'download':
        if (not self.connected):
            rety = self.connect()
            # print('try connect: ', rety)
            if not rety:
                # print('can not connect. ')
                return []
        if not self.dir_checked:
            rety = self.ensure_remote_dir_exists()
            if not rety:
                log.log('#61, dir check have failed. %s' % (self.remote_dir))
                print('dir check have failed. %s' % (self.remote_dir))
                return []
        # # print(type(files_to_process))
        # # print('start files_in_dir')   start connection.
        files=[]
        files_to_process = []
        try:
            # # print(files)
            if self.type == 'upload':
                # try:
                #     os.stat(self.local_dir)
                # except:
                #     os.mkdir(self.local_dir)
                #     log.log("no local dir find: %s" % self.local_dir)
                # it make no sense, since this folder should be created by those who put file in it.

                # folders = [fd for fd in glob.glob(self.local_dir) if os.path.isdir(fd)]
                # acc = []
                # for fd in folders:
                #     files_in_fd = os.listdir(fd)
                #     one_folder_files_pair = [[os.path.join(fd,f), os.path.join(self.remote_dir, f)] for f in files_in_fd
                #                              if ((not (f.startswith('.') or f.startswith('#') or os.path.isdir(os.path.join(fd, f))))
                #                                  and self.regex.match(f))]
                #     acc += one_folder_files_pair

                dir_files_not_checked = findfiles.FindFiles(self.local_dir, self.file, recursive=self.recursive)
                # TODO: delay check to make sure file is finished writing or deleted.
                file_check_flag = True
                mtimes_last = findfiles.FindMTime(dir_files_not_checked)
                msizes_last = findfiles.FindSize(dir_files_not_checked)
                time.sleep(3)
                mtimes_this = findfiles.FindMTime(dir_files_not_checked)
                msizes_this = findfiles.FindSize(dir_files_not_checked)
                mtimes_compare = [i[0] if (i[0] == i[1] and i[2] == i[3]) else False for i in zip(mtimes_last, mtimes_this, msizes_last, msizes_this)]
                dir_files = [i[0] for i in zip(dir_files_not_checked, mtimes_compare) if i[1]]
                # mtimes = findfiles.FindMTime(dir_files)
                mtimes = [i for i in mtimes_compare if i]

                acc_notsorted = [[f, os.path.join(self.remote_dir, os.path.split(f)[-1])] for f in dir_files]
                if str(self.order) == "new":
                    zip_sorted = sorted(zip(acc_notsorted, mtimes), key=lambda x: x[1], reverse=True)
                    acc = [i[0] for i in zip_sorted]
                elif str(self.order) == "old":
                    zip_sorted = sorted(zip(acc_notsorted, mtimes), key=lambda x: x[1], reverse=False)
                    acc = [i[0] for i in zip_sorted]
                elif str(self.order) == 'desc':
                    acc = sorted(acc_notsorted, key=lambda x: x[0], reverse=True)
                elif str(self.order) == 'asc':
                    acc = sorted(acc_notsorted, key=lambda x: x[0], reverse=False)
                elif str(self.order) == 'mix':
                    zip_new = sorted(zip(acc_notsorted, mtimes), key=lambda x: x[1], reverse=True)
                    zip_old = sorted(zip(acc_notsorted, mtimes), key=lambda x: x[1], reverse=False)
                    acc = [i[0] for j in zip(zip_new, zip_old) for i in j][:len(zip_new)]
                files_to_process = acc
                try:
                    if not self.limit == '-1':
                        print('set files number limit %s' % (self.limit))
                        files_to_process = acc[:min(len(acc), int(self.limit))]
                except:
                    log.log("#44, fail to set files limit.")
                    pass
                # files_to_process = [[os.path.join(self.local_dir,f), os.path.join(self.remote_dir, f)] for f in files if ((not (f.startswith('.') or os.path.isdir(os.path.join(self.local_dir, f)))) and self.regex.match(f))]
            elif self.type == 'download':
                # if self.protocol == 'sftp':
                #     try:
                #         files = self.sftp.listdir(self.remote_dir)
                #         files_to_process = [[os.path.join(self.remote_dir,f), os.path.join(self.local_dir, f)] for f in files
                #                             if ((not (f.startswith('.') or f.startswith('#') or os.path.isdir(os.path.join(fd, f))))
                #                                 and self.regex.match(f))]
                #     except:
                #         pass
                # if self.protocol == 'ftp':
                #     try:
                #         files = self.ftp.nlst()
                #         files_to_process = [[os.path.join(self.remote_dir,f), os.path.join(self.local_dir, f)] for f in files
                #                             if ((not (f.startswith('.') or f.startswith('#') or os.path.isdir(os.path.join(fd, f))))
                #                                 and self.regex.match(f))]
                #     except:
                #         pass
                # if self.protocol == 'mv' or self.protocol == 'local':
                #     folders = [fd for fd in glob.glob(self.remote_dir) if os.path.isdir(fd)]
                #     acc = []
                #     for fd in folders:
                #         one_folder_files_pair = [[os.path.join(fd,f), os.path.join(self.local_dir, f)] for f in os.listdir(fd)
                #                                  if ((not (f.startswith('.') or f.startswith('#') or os.path.isdir(os.path.join(fd, f))))
                #                                      and self.regex.match(f))]
                #         acc += one_folder_files_pair

                if self.protocol == 'sftp':
                    os_method = self.sftp.listdir
                elif self.protocol == 'ftp':
                    os_method = lambda x: self.ftp.nlst()
                elif self.protocol == 'mv' or self.protocol == 'local':
                    os_method = os.listdir
                dir_files = findfiles.FindFiles(self.remote_dir, self.file, os_method=os_method)
                acc = [[f, os.path.join(self.local_dir, os.path.split(f)[-1])] for f in dir_files]

                files_to_process = acc
                try:
                    if not os.path.isdir(self.local_dir):
                        # print('making dir')
                        os.makedirs(self.local_dir)
                except:
                    log.log("can not create local dirs:%s"%(str(sys.exc_info())))
                    pass
            # print(files_to_process)
            return files_to_process
        except:
            log.log("#31, Unexpected error:%s"% (str(sys.exc_info())))
            return files_to_process


    def process(self):
        flag = False
        try:
            # self.connected = self.connect()
            # self.dir_checked = self.ensure_remote_dir_exists()
            flag = self.process_file(self.files_in_dir())
            self.close()
            # who opens it, who close it.
            # one function do one thing princeple
        except Exception as e:
            log.log('#11, process function exception %s'%(str(repr(e))))
            self.close()
        return flag


    def __enter__(self):
        return self
    def __exit__(self, type, value):
        self.close()

# end of class Server.


def work_on(cfg_dict):
    for k, v in cfg_dict.items():
        print('###################### Now on operation %s ######################' % k)
        try:
            s = Server(v)
            s.process()
            del s
        except:
            continue

# =============================================================================
# todos
# =============================================================================
# DONE: template file name, and rename.  or just check the file complety. or check longth.
# TODO: while true loop. and operate use try except. time.sleep()
# TODO: mutiply lines.  Maybe not needed.
# waiting: go  thread someting.
# TODO: also, log files. DONE
# TODO: deleting method have huge floaw.

# =============================================================================
# test examples:
# =============================================================================
# o4 = Server(cfg['mission4'])
# o4.process()
# o4.connect()
# fs = o4.files_in_dir()
# o4.close()

# o1 = Server(cfg['mission1'])
# o1.process()
# o1.connect()
# fs = o1.files_in_dir()
# o1.close()


def main(cfg={}, sleepfor=15):
    try:
        if not cfg:
            import parse_argv
            import read_config
            d = parse_argv.parse_argv()
            cfg = read_config.ReadConfig_DB(d['cfg']).check_config(convert_port=True)
            print("parse_argv_d:\n%s"%(d))
            print("config:\n%s"%(cfg))
            if 'sleepfor' in d:
                sleepfor = parse_argv.parse_time_length(d['sleepfor'])
        while True:
            # work_on(config)
            work_on(cfg)
            # print("###################### Now On Sleep for %s s ######################" % sleepfor)
            time.sleep(sleepfor)
    except KeyboardInterrupt:
        print('please use "--cfg" to set config file path, "--sleepfor" to set mission interval. ')
        log('interrupted!')
        raise


if __name__ == '__main__':
    main()
