#!/bin/bash

if [ "$1" == ""  ]
then
    echo "transfers products to HDFS"
    echo "usage  : $0 <source-dir>"
    echo "example: $0 /hd3"
    echo "creates e.g. hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2010/11/17"
    exit 1
fi

baseDir="`dirname ${0}`"
configDir=${baseDir}/conf
jobJar=${baseDir}/calvalus-experiments-0.1-SNAPSHOT-job.jar

sourceDir=$1
sourceName=`basename ${sourceDir}`
now=`date '+%Y-%m-%dT%H:%M:%S'`

time hadoop --config ${configDir} jar ${jobJar} com.bc.calvalus.ingestion.IngestionTool ${sourceDir} > transfer-${sourceName}-${now}.tmp 2>&1

echo transfer-${sourceName}-${now}.tmp
