#!/bin/bash

if [ "$1" == "" ]
then
    echo "converts beam processing output to DIMAP format (and copies from cluster)"
    echo "usage  : $0 <sequencefileinput> <dimapoutput>"
    echo "example: $0 hdfs://cvmaster00:9000/calvalus/outputs/meris-l2beam-99/L2_of_MER_RR__1P.N1.seq /tmp/meris-l2beam-88/MER_RR__2P"
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
output=${2:-$1}

#echo hadoop jar ${jobJar} com.bc.calvalus.processing.beam.BeamOutputConverterTool ${input} ${output} $@
time hadoop jar ${jobJar} com.bc.calvalus.processing.beam.BeamOutputConverterTool ${input} ${output} $@

