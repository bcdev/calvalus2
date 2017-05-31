#!/bin/bash

set -e

output=$1
composite=$2
name=$(basename ${output})
tile=${name:0:6}
year=${name:7:4}
echo $tile
echo $year

for month in 1 2 3 4 5 6 7 8 9 10 11 12; do
    tar xvf ${output} ${year}/clasification_CLOSING_V2.1_${month}_${year}_${tile}.tif
    tar xvf ${output} ${year}/Uncertainty_V2.1_${month}_${year}_${tile}.tif
    tar xvf ${composite} ${year}/MOD${year}_${tile}_${month}_date.tif

    java -cp calvalus-processing.jar com.bc.calvalus.processing.fire.format.grid.modis.Merger burned_${year}_${month}_${tile}.nc ${year}/clasification_CLOSING_V2.1_${month}_${year}_${tile}.tif ${year}/Uncertainty_V2.1_${month}_${year}_${tile}.tif ${year}/MOD${year}_${tile}_${month}_date.tif

done