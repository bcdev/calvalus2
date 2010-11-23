#!/bin/bash

if [ "$1" == "" ]
then
    echo "submits Hadoop job to Job tracker"
    echo "usage  : $0 <request>"
    echo "example: $0 l2gen-request.xml"
    exit 1
fi

baseDir="`dirname ${0}`/.."
configDir=${baseDir}/src/main/admin/conf
jobJar=${baseDir}/target/calvalus-experiments-0.1-SNAPSHOT-job.jar

commandline="$0 $*"
command=$0
request=$1
shift 1
now=`date '+%Y-%m-%dT%H:%M:%S'`
logfile=exejob-`basename ${request%.xml}`-${now}

echo ${commandline}

#time hadoop --config ${configDir} jar ${jobJar} com.bc.calvalus.experiments.executables.ExecutablesTool ${request} $@ > ${logfile}.tmp 2>&1
echo hadoop jar ${jobJar} com.bc.calvalus.experiments.executables.ExecutablesTool ${request} $@
time hadoop jar ${jobJar} com.bc.calvalus.experiments.executables.ExecutablesTool ${request} $@ > ${logfile}.tmp 2>&1

job_number=`cat ${logfile}.tmp | awk "/ Running job: / { print substr(\\$7,5) }"`

echo ${job_number}
