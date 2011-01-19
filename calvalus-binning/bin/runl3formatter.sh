#!/bin/bash

if [ "$1" == "" ]
then
    echo "submits Hadoop l3 job to Job tracker"
    echo "usage  : $0 <dest-url>"
    echo "example: $0 hdfs://cvmaster00:9000/output/"
    exit 1
fi

baseDir="`dirname ${0}`/.."
configDir=${baseDir}/src/main/admin/conf
jobJar=${baseDir}/target/calvalus-experiments-0.1-SNAPSHOT-job.jar

commandline="$0 $*"
now=`date '+%Y-%m-%dT%H:%M:%S'`
logfile=l3job-${now}

echo ${commandline}

echo hadoop --config ${configDir} jar ${jobJar} com.bc.calvalus.binning.job.L3Formatter $@
time hadoop --config ${configDir} jar ${jobJar} com.bc.calvalus.binning.job.L3Formatter $@ 2>&1 |tee ${logfile}.tmp

job_number=`cat ${logfile}.tmp | awk "/ Running job: / { print substr(\\$7,5) }"`

echo ${job_number}
