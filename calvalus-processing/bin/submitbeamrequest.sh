#!/bin/bash

if [ "$1" == "" ]
then
    echo "submits Hadoop job to Job tracker"
    echo "usage  : $0 <request>"
    echo "example: $0 beam-l2-sample-request.xml"
    exit 1
fi

baseDir="`dirname ${0}`/.."
baseDir=`( cd $baseDir ; pwd )`
jobJar=${baseDir}/lib/calvalus-processing-0.1-SNAPSHOT-job.jar
if [ ! -r $jobJar ] ; then
    # maven development environment
    jobJar=${baseDir}/target/calvalus-processing-0.1-SNAPSHOT-job.jar
fi

cp1=`echo ${baseDir}/lib/beam/*.jar | tr ' ' ':'`
cp2=`echo ${baseDir}/lib/saxon*.jar | tr ' ' ':'`
export HADOOP_CLASSPATH=$cp1:$cp2

#echo hadoop jar ${jobJar} com.bc.calvalus.processing.beam.BeamOperatorTool $@
time hadoop jar ${jobJar} com.bc.calvalus.processing.beam.BeamOperatorTool $@
