#!/bin/bash

BIN_DIR=`dirname $0`
(
for i in 1 2 3
do
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-product n1 c2r
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-product n3 c2r
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-product n3 c2r -splits=18
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/sliced/MERIS-RR-product sliced c2r
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/lineinterleaved/MERIS-RR-product lineinterleaved c2r

    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-2010-08 n1 c2r
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-2010-08 n3 c2r
#    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-2010-08 n3 c2r -splits=18
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/sliced/MERIS-RR-2010-08 sliced c2r
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/lineinterleaved/MERIS-RR-2010-08 lineinterleaved c2r
done
) 2>&1 | tee output-`date '+%Y-%m-%dT%H:%M:%S'`.list 

