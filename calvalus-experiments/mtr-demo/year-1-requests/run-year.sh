#!/bin/bash

basedir=`dirname $0`
now=`date '+%Y-%m-%dT%H:%M:%S'`
logfile=$basedir/year-${now}.log

{
date
for d in `cat $basedir/start-dates.txt`
do
    echo "/home/boe/modules/calvalus/calvalus-experiments/bin/runl3tool.sh $basedir/${d}.properties"
    /home/boe/modules/calvalus/calvalus-experiments/bin/runl3tool.sh $basedir/${d}.properties
    date
done
} 2>&1 | tee $logfile

