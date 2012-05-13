#!/bin/bash

BIN_DIR=`dirname $0`
(
for i in 00 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18
do
    echo cvslave$i
    ssh hadoop@cvslave${i} mkdir ingestion
    scp calvalus-experiments-0.4-SNAPSHOT-job.jar ../bin/ingest.sh hadoop@cvslave${i}:ingestion
    ssh hadoop@cvslave${i} "chmod +x ingestion/ingest.sh ; cd ingestion ; ln -s ../conf"
done
) 2>&1 | tee output-`date '+%Y-%m-%dT%H:%M:%S'`.list

