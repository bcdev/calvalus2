#!/bin/bash

(
for i in 1 2 3
do
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/n1/MERIS-RR-product n1 ndvi
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/n1/MERIS-RR-product n3 ndvi
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/n1/MERIS-RR-product n3 ndvi -splits=18
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/sliced/MERIS-RR-product sliced ndvi
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/lineinterleaved/MERIS-RR-product lineinterleaved ndvi

../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/n1/MERIS-RR-2010-08 n1 ndvi
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/n1/MERIS-RR-2010-08 n3 ndvi
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/n1/MERIS-RR-2010-08 n3 ndvi -splits=18
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/sliced/MERIS-RR-2010-08 sliced ndvi
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/lineinterleaved/MERIS-RR-2010-08 lineinterleaved ndvi

../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/n1/MERIS-RR-product n1 radiometry
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/n1/MERIS-RR-product n3 radiometry
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/n1/MERIS-RR-product n3 radiometry -splits=18
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/sliced/MERIS-RR-product sliced radiometry
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/lineinterleaved/MERIS-RR-product lineinterleaved radiometry

../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/n1/MERIS-RR-2010-08 n1 radiometry
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/n1/MERIS-RR-2010-08 n3 radiometry
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/n1/MERIS-RR-2010-08 n3 radiometry -splits=18
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/sliced/MERIS-RR-2010-08 sliced radiometry
../bin/runprocessing2 hdfs://cvmaster00:9000/data/experiments/lineinterleaved/MERIS-RR-2010-08 lineinterleaved radiometry
done
) 2>&1 | tee output.list 

