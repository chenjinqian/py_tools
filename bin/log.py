import os
import time


# log_path_default = '/media/mmcblk0p1/log/'
log_path_default = './log/'
def log(log_mess = '', log = 'main_log', print2file = True ):
    try:
        if print2file:
            if not os.path.isdir(log_path_default + log):
                os.makedirs(log_path_default + log)
            nt_file = time.strftime('%Y%m%d.txt')
            f = file(log_path_default + log + '/' + nt_file, 'ab')
            f.write(time.strftime('%Y%m%d_%H%M%S      '))
            f.write(log_mess)
            f.write('\n')
        else:
            print '[%s]%s      %s'%(log, time.strftime('%Y%m%d_%H%M%S'), log_mess)
        for ff in os.listdir(log_path_default + log):
            try:
                ft = time.mktime(time.strptime(ff, '%Y%m%d.txt'))
                if time.time() - ft > 3600*24*2:
                    os.unlink(log_path_default + log + '/' + ff)
            except:
                pass
    except:
        pass
