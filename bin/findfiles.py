#!/usr/bin/python

# Author: ChenJinQian
# Email:  2012chenjinqian@gmail.com

import os
import re

def FindFiles(folders, filters='.*', recursive = False, os_method=os.listdir):
    """find files in a list of folder using one or more regex match,
    with option to recursive down folder or not."""
    files_acc = []
    dir_acc = []
    if not type(folders) == list:
        folders = [folders]
    if not type(filters) == list:
        filters = [filters]
        # print('new filters (list) %s ' % filters)
    if folders == []:
        return []
    for fd in folders:
        try:
            sub_dirpack = [os.path.join(fd, d)
                           for d in os_method(fd)
                           if os.path.isdir(os.path.join(fd,d))]
            dir_acc = dir_acc + sub_dirpack
        except:
            continue
        for fl in filters:
            try:
                filepack = [os.path.join(fd, f)
                            for f in os_method(fd)
                            if re.match(fl, f)]
                files_acc = files_acc + filepack
            except:
                continue
    if not recursive:
        return files_acc  # in full path
    else:
        return files_acc + FindFiles(dir_acc, filters, recursive, os_method)


def FindSize(files, os_method=os.path.getsize):
    # sizes = [os_method(i) for i in files]
    sizes = []
    for i in files:
        try:
            size = os_method(i)
            sizes.append(size)
        except:
            sizes.append(None)
    return sizes


def FindMTime(files, os_method=os.path.getmtime):
    # mtimes = [os_method(i) for i in files]
    mtimes = []
    for i in files:
        try:
            mtime = os_method(i)
            mtimes.append(mtime)
        except:
            mtimes.append(None)
    return mtimes


def FindStaticFiles(folder, filters, recursive=False, os_method=os.listdir, sleepfor=3):
    files = FindFiles(folder, filters, recursive, os_method)
    mtimes_last = FindMTime(files)
    msizes_last = FindSize(files)
    import time
    time.sleep(sleepfor)
    mtimes_last = FindMTime(files)
    msizes_last = FindSize(files)
    mtimes_compare = [i[0] if (i[0] == i[1] and i[2] == i[3])
                      else False
                      for i in zip(mtimes_last, mtimes_this, msizes_last, msizes_this)]
    dir_files = [i[0] for i in zip(dir_files_not_checked, mtimes_compare) if i[1]]
    # mtimes = findfiles.FindMTime(dir_files)
    mtimes = [i for i in mtimes_compare if i]
    return dir_files


def SortFiles(files, option='new', os_method=os.path.getmtime):
    mtimes = FindMTime(files, os_method=os_method)
    acc = files
    acc_notsorted = files
    if str(self.order) == "new":
        zip_sorted = sorted(zip(acc_notsorted, mtimes), key=lambda x: x[1], reverse=True)
        acc = [i[0] for i in zip_sorted]
    elif str(self.order) == "old":
        zip_sorted = sorted(zip(acc_notsorted, mtimes), key=lambda x: x[1], reverse=False)
        acc = [i[0] for i in zip_sorted]
    elif str(self.order) == 'desc':
        acc = sorted(acc_notsorted, key=lambda x: x, reverse=True)
    elif str(self.order) == 'asc':
        acc = sorted(acc_notsorted, key=lambda x: x, reverse=False)
    elif str(self.order) == 'mix':
        zip_new = sorted(zip(acc_notsorted, mtimes), key=lambda x: x[1], reverse=True)
        zip_old = sorted(zip(acc_notsorted, mtimes), key=lambda x: x[1], reverse=False)
        acc = [i[0] for j in zip(zip_new, zip_old) for i in j][:len(zip_new)]
    return acc


def main():
    print("""find files in a list of folder,\
             using one or more regex match,\
             with option to recursive down folder or not(default).\n""")
    print("FindFiles(['/home/lcg/pro/tools/'],\
                     ['.*\.py$', '.txt$'],\
                     recursive=True)")
    print(FindFiles(['/home/lcg/pro/tools/'],
                    ['.*\.py$', '.txt$'],
                    recursive=True))


if __name__ == '__main__':
    main()
