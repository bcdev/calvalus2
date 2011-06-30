import os
import sys
import datetime

# usage: python productlist.py /some/root/dir | python productsoverlapping.py >> overlaps ; done
# MER_FSG_1PNMAP20061111_151656_000000702052_00469_24571_0001.N1

def start(f):
    return datetime.datetime(int(f[14:18]), int(f[18:20]), int(f[20:22]), int(f[23:25]), int(f[25:27]), int(f[27:29]))


def stop(f):
    start = datetime.datetime(int(f[14:18]), int(f[18:20]), int(f[20:22]), int(f[23:25]), int(f[25:27]), int(f[27:29]))
    duration = datetime.timedelta(0, int(f[30:38]))
    return start + duration

lines = sys.stdin.readlines()
files = map(lambda x:x[x.find(' ') + 1:-1], lines)
dirs = map(lambda x:x[0:x.find(' ')], lines)

for i in range(len(files)):
    for j in range(i + 1, len(files)):
        if start(files[j]) >= stop(files[i]):
            break;
            # overlapping but not covering: look for smaller one
        if start(files[j]) > start(files[i]) and stop(files[j]) > stop(files[i]):
            if stop(files[i]) - start(files[i]) > stop(files[j]) - start(files[j]):
                if (stop(files[j]) - stop(files[i]) <= datetime.timedelta(0, 5)):
                    print 'rm', dirs[j] + '/' + files[
                                                j] #, files[i], stop(files[i]) - start(files[j]), stop(files[j]) - stop(files[i])
            else:
                if (start(files[j]) - start(files[i]) <= datetime.timedelta(0, 5)):
                    print 'rm', dirs[i] + '/' + files[
                                                i] #, files[j], stop(files[i]) - start(files[j]), start(files[j]) - start(files[i])
