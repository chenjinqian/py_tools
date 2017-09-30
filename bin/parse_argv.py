"""
Two usefull parse argument functions.
"""
import os, sys, re

def parse_argv():
    """Get sys.argv and convert it into dic.
    Value can be string or list of string."""
    import re
    argv_lst = sys.argv
    d = {}
    key = '__file__'
    d[key] = None

    def item_pack(a, b):
        if a is None:
            return b
        elif type(a) == list:
            a.append(b)
            return a
        else:
            return [a, b]

    for s in argv_lst:
        # print('s is %s' % s)
        match = re.match('^-{1,2}(.+)$', s)
        if match:
            key = match.group(1)
            d[key] = None
        else:
            # print(d[key], s, item_pack(d[key], s))
            # if key == 'app':
            #     d[key] = item_pack(d[key], 'mysql:app_%s' % s)
            # else:
            #     d[key] = item_pack(d[key], s)
            d[key] = item_pack(d[key], s)
    return d


def parse_time_length(s):
    """Convert human read-able string into int,
    such as 1h = 3600 (second).
    Support things such as 2.4d, .5h etc.
    Return None is string illegle."""
    try:
        length = int(s)
        return length
    except:
        # return None
        pass
    try:
        factor = {}
        factor['Y'] = int(60 * 60 * 24 * 365)
        factor['M'] = int(60 * 60 * 24 * 30)
        factor['d'] = int(60 * 60 * 24)
        factor['h'] = int(60 * 60)
        factor['m'] = int(60)
        factor['s'] = int(1)
        s_number = '0123456789.'
        length = 0
        l = '0'
        for i in s:
            # print('i:%s'%(i))
            if i in s_number:
                l = l + i
                # print(l)
                continue
            if i in factor:
                length += float(l) * int(factor[i])
                l = '0'
                continue
            print("symbol '%s' not recongnised, please use symbol of Y/M/d/h/m/s."%(i))
            return None
        rlt = int(length + int(float(l)))
        return rlt
    except:
        print('bad length format.')
        return None


def parse_range(s):
    """
    input: '6-9,14-17'
    output: [6, 7, 8, 9, 14, 15, 16, 17 ]
    """
    if not s:
        print("#a, illegal string %s"%(s))
        return []
    a_to_b_list = [i for i in s.split(',') if i]
    acc = []
    for ab in a_to_b_list:
        if '-' in ab:
            ab_lst = [j for j in ab.split('-') if j]
            try:
                a, b = ab_lst
                acc += range(int(a), int(b)+1)
            except Exception as e:
                # print("ab_lst:%s, a:%s, b:%s"%(ab_lst, a, b))
                print("#b, illegal string %s, ERROR:%s"%(s, repr(e)))
                continue
        else:
            try:
                acc.append(int(ab))
            except:
                print("#c, illegal string %s"%(s))
                continue
    return acc
