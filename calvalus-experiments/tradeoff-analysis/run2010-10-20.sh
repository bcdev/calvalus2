#!/bin/bash

BIN_DIR=`dirname $0`
(
for i in 1 2 3
do
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-product n1 ndvi
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-product n3 ndvi
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-product n3 ndvi -splits=18
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/sliced/MERIS-RR-product sliced ndvi
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/lineinterleaved/MERIS-RR-product lineinterleaved ndvi

    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-2010-08 n1 ndvi
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-2010-08 n3 ndvi
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-2010-08 n3 ndvi -splits=18
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/sliced/MERIS-RR-2010-08 sliced ndvi
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/lineinterleaved/MERIS-RR-2010-08 lineinterleaved ndvi

    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-product n1 radiometry
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-product n3 radiometry
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-product n3 radiometry -splits=18
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/sliced/MERIS-RR-product sliced radiometry
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/lineinterleaved/MERIS-RR-product lineinterleaved radiometry

    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-2010-08 n1 radiometry
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-2010-08 n3 radiometry
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/n1/MERIS-RR-2010-08 n3 radiometry -splits=18
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/sliced/MERIS-RR-2010-08 sliced radiometry
    $BIN_DIR/runprocessing2 hdfs://master00:9000/data/experiments/lineinterleaved/MERIS-RR-2010-08 lineinterleaved radiometry
done
) 2>&1 | tee output-`date '+%Y-%m-%dT%H:%M:%S'`.list 

