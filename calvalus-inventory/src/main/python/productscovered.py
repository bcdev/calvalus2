import os
import sys
import datetime

# usage: python productlist.py /some/root/dir | python productscovered.py >> covered ; done
# ./region/year/month/day/MER_FSG_1PNMAP20061111_151656_000000702052_00469_24571_0001.N1

def start(f):
    return datetime.datetime(int(f[14:18]), int(f[18:20]), int(f[20:22]), int(f[23:25]), int(f[25:27]), int(f[27:29]))

def stop(f):
    start = datetime.datetime(int(f[14:18]), int(f[18:20]), int(f[20:22]), int(f[23:25]), int(f[25:27]), int(f[27:29]))
    duration = datetime.timedelta(0,int(f[30:38]))
    return start + duration

files = sys.stdin.readlines()
files = map(lambda x:x[:-1], files)

for i in range(len(files)):
    pos = files[i].rindex('/')
    pos1 = files[i].rindex('/',0,pos)
    pos2 = files[i].rindex('/',0,pos1)
    pos3 = files[i].rindex('/',0,pos2)
    pos4 = files[i].rindex('/',0,pos3)
    filei = files[i][pos+1:]
    regioni = files[i][pos4+1:pos3]
    for j in range(i+1,len(files)):
        pos = files[j].rindex('/')
        pos1 = files[j].rindex('/',0,pos)
        pos2 = files[j].rindex('/',0,pos1)
        pos3 = files[j].rindex('/',0,pos2)
        pos4 = files[j].rindex('/',0,pos3)
        filej = files[j][pos+1:]
        regionj = files[j][pos4+1:pos3]
        # j is different region
        if regionj != regioni:
            break;
        # j starts after stop of i: next i
        if start(filej) >= stop(filei):
            break;
        # same start date: check both directions
        if start(filej) == start(filei):
            if stop(filej) <= stop(filei):
                print regionj, filej, filei, stop(filej) - start(filej), stop(filei) - start(filei)
            else:
                print regioni, filei, filej, stop(filei) - start(filei), stop(filej) - start(filej)
        # i starts earlier: check whether i stops not earlier
        elif stop(filej) <= stop(filei):
            print regionj, filej, filei, stop(filej) - start(filej), stop(filei) - start(filei)
