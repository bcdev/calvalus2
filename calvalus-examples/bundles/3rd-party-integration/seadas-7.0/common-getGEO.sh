#!/bin/bash
#
# loads MODIS geofile into current working dir
#
set -e

# hdfs://foo:098/bar/foo.txt  -> foo.txt
function filename {
  echo ${1##*/}
}

# /bar/foo.txt  -> /bar
# hdfs://foo:098/bar/foo.txt  -> hdfs://foo:098/bar
function parentdir {
  echo ${1%/*}
}

# A2003029000500.L1B_LAC -> A2003029000500
# A2003029000500.L1A_LAC.bz2 -> A2003029000500
function granulename {
  echo ${1%.L1*}
}

# A2003029000500  -> 2003/01/29
function datetree {
  local pyear=${1:1:4}
  local pday=${1:5:3}
  echo $(date -d "${pyear}-01-01 +${pday} days -1 day" "+%Y/%m/%d")
}

function from_hdfs {
  local path=$1
  local granule=$2
  echo "testing HDFS ${path}/${granule}.GEO"
  local hadoopLs=$(hadoop fs -ls ${path}/${granule}.GEO*)
  local hadoopLsArray=(${hadoopLs})
  local geoPath=${hadoopLsArray[7]}
  if [ "$geoPath" != "" ]; then
    echo "copy to CWD: $geoPath -> ${granule}.GEO"
    hadoop fs -copyToLocal $geoPath ${granule}.GEO
    return 0
  else
    return 1
  fi
}

inputPath=$1
filename=$(filename ${inputPath})
granulename=$(granulename ${filename})

# test same directory
# test /calvalus/eodata/MODISA_GEO/v1/${productDate}/${geoFile}*"
# test PML
#    geopaths = [
#      the_date.strftime("/data/datasets/operational/aqua_modis/level1a-geo/nasa_obpg-seadas6.4/swath/0d/%Y/%m/%d/hdf/A%Y%j" + time + ".GEO_LAC"),
#      the_date.strftime("/data/datasets/operational/aqua_modis/level1a-geo/nasa_obpg-seadas6.4/swath/0d/%Y/%m/%d/hdf/A%Y%j" + time + ".GEO"),
#      the_date.strftime("/data/datasets/operational/aqua_modis/level1a-geo/nasa_obpg-seadas6.4/swath/%Y/%m/%d/hdf/A%Y%j" + time + ".GEO_LAC"),
#      the_date.strftime("/data/datasets/operational/aqua_modis/level1a-geo/nasa_obpg-seadas6.4/swath/%Y/%m/%d/hdf/A%Y%j" + time + ".GEO"),
#      the_date.strftime("/data/datasets/operational/aqua_modis/level1a-geo/nasa_obpg-seadas7.0.1/swath/0d/%Y/%m/%d/A%Y%j" + time + ".GEO_LAC"),
#      the_date.strftime("/data/datasets/operational/aqua_modis/level1a-geo/nasa_obpg-seadas7.0.1/swath/0d/%Y/%m/%d/A%Y%j" + time + ".GEO")
#    ]

if from_hdfs $(parentdir ${inputPath}) ${granulename}; then
  exit 0
fi
if from_hdfs /calvalus/eodata/MODISA_GEO/v1/$(datetree ${granulename}) ${granulename}; then
  exit 0
fi
echo "failed get required GEO file"
exit 1

