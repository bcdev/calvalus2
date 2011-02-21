#!/bin/bash

if [ "$1" == "" ]
then
    echo "converts beam Level 3 processing output to product or image format (and copies from cluster)"
    echo "usage  : $0 format-request.xml"
    echo "example: $0 my-l3-formatting.xml"
    exit 1
fi

baseDir="`dirname ${0}`/.."
baseDir=`( cd $baseDir ; pwd )`
jobJar=${baseDir}/lib/calvalus-processing-0.1-SNAPSHOT-job.jar
if [ ! -r $jobJar ] ; then
    # maven development environment
    jobJar=${baseDir}/target/calvalus-processing-0.1-SNAPSHOT-job.jar
fi

export HADOOP_CLASSPATH=`echo ${baseDir}/lib/beam/* | tr ' ' ':'`

input=$1
shift

#echo hadoop jar ${jobJar} com.bc.calvalus.processing.beam.L3Formatter ${input}  $@
time hadoop jar ${jobJar} com.bc.calvalus.processing.beam.L3Formatter ${input}  $@

