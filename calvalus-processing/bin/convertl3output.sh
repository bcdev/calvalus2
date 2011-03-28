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

 cp1=`echo ${baseDir}/lib/beam/*.jar | tr ' ' ':'`
 cp2=`echo ${baseDir}/lib/saxon*.jar | tr ' ' ':'`
 export HADOOP_CLASSPATH=$cp1:$cp2

input=$1
shift

#echo hadoop jar ${jobJar} com.bc.calvalus.processing.beam.L3FormatterTool ${input}  $@
time hadoop jar ${jobJar} com.bc.calvalus.processing.beam.L3FormatterTool ${input}  $@

