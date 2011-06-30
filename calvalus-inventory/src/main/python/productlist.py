import os
import sys

fileList = []
rootdir = sys.argv[1]
for root, subFolders, files in os.walk(rootdir):
    for file in files:
        if file.endswith('.gz'):
            fileList.append(rootdir + ' ' + file)

fileList.sort()

for f in fileList:
    print f
