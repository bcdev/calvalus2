#!/bin/bash

if [ "$1" == "" ]
then
    echo "submits Hadoop job to Job tracker"
    echo "usage  : $0 <request>"
    echo "example: $0 l2gen-request.xml"
    exit 1
fi

baseDir="`dirname ${0}`/.."
baseDir=`( cd $baseDir ; pwd )`
configDir=${baseDir}/conf
jobJar=${baseDir}/lib/calvalus-processing-0.1-SNAPSHOT-job.jar
if [ ! -r $jobJar ] ; then
    # maven development environment
    jobJar=${baseDir}/target/calvalus-processing-0.1-SNAPSHOT-job.jar
fi

request=$1

echo hadoop --config ${configDir} jar ${jobJar} com.bc.calvalus.processing.shellexec.ExecutablesTool ${request} $@
time hadoop --config ${configDir} jar ${jobJar} com.bc.calvalus.processing.shellexec.ExecutablesTool ${request} $@
