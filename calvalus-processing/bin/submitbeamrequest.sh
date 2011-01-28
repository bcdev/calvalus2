#!/bin/bash

if [ "$1" == "" ]
then
    echo "submits Hadoop job to Job tracker"
    echo "usage  : $0 <request>"
    echo "example: $0 beam-l2-sample-request.xml"
    exit 1
fi

baseDir="`dirname ${0}`/.."
configDir=${baseDir}/bin/conf
jobJar=${baseDir}/target/calvalus-processing-0.1-SNAPSHOT-job.jar

commandline="$0 $*"
command=$0
request=$1
shift 1
now=`date '+%Y-%m-%dT%H:%M:%S'`
logfile=beamjob-`basename ${request%.xml}`-${now}

echo ${commandline}

echo hadoop --config ${configDir} jar ${jobJar} com.bc.calvalus.processing.beam.BeamOperatorTool ${request} $@
time hadoop --config ${configDir} jar ${jobJar} com.bc.calvalus.processing.beam.BeamOperatorTool ${request} $@ > ${logfile}.tmp 2>&1

job_number=`cat ${logfile}.tmp | awk "/ Running job: / { print substr(\\$7,5) }"`

echo ${job_number}
