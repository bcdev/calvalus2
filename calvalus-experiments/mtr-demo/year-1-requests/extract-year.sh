#!/bin/bash

basedir=`dirname $0`
now=`date '+%Y-%m-%dT%H:%M:%S'`
logfile=$basedir/year-${now}.log

date
for d in `cat $basedir/start-dates.txt`
do
    /home/boe/modules/calvalus/calvalus-experiments/bin/runl3formatter.sh $basedir/${d}.properties
done

