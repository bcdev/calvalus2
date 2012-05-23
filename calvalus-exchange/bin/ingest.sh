#!/bin/bash

#if [ "$1" == ""  ]
#then
#    echo "transfers products to HDFS"
#    echo "usage  : $0 ( <source-dir> | <source-files> ) [-producttype=<productType>] [-revision=<revision>] [-replication=<replication>] [-blocksize=<blocksize>]"
#    echo "example: $0 /hd3"
#    echo "creates e.g. hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2010/11/17"
#    echo "example: $0 /hd3 -producttype=MER_FRS_1P -revision=cc -replication=2"
#    echo "creates e.g. hdfs://master00:9000/calvalus/eodata/MER_FRS_1P/cc/2010/11/17"
#
#    exit 1
#fi

baseDir="`dirname ${0}`/.."
configDir=${baseDir}/conf
jar=${baseDir}/lib/calvalus-exchange-1.3-SNAPSHOT.jar

sourceDir=$1
sourceName=`basename ${sourceDir}`
now=`date '+%Y-%m-%dT%H:%M:%S'`

time hadoop --config ${configDir} jar ${jar} com.bc.calvalus.ingestion.IngestionTool $@ > transfer-${sourceName}-${now}.tmp 2>&1

echo transfer-${sourceName}-${now}.tmp
