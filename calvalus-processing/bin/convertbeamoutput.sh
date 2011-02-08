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
configDir=${baseDir}/conf
jobJar=${baseDir}/lib/calvalus-processing-0.1-SNAPSHOT-job.jar
if [ ! -r $jobJar ] ; then
    # maven development environment
    jobJar=${baseDir}/target/calvalus-processing-0.1-SNAPSHOT-job.jar
fi

export HADOOP_CLASSPATH=`echo ${baseDir}/lib/beam/* | tr ' ' ':'`

input=$1
output=${2:-$1}

#echo hadoop --config ${configDir} jar ${jobJar} com.bc.calvalus.processing.beam.BeamOutputConverterTool ${input} ${output} $@
time hadoop --config ${configDir} jar ${jobJar} com.bc.calvalus.processing.beam.BeamOutputConverterTool ${input} ${output} $@

