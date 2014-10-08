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
  echo ${1:0:14}
}

# A2003029000500  -> 2003/01/29
function datetree {
  local pyear=${1:1:4}
  local pday=${1:5:3}
  echo $(date -d "${pyear}-01-01 +${pday} days -1 day" "+%Y/%m/%d")
}
# hadoop fs -ls /calvalus/eodata/MODISA_GEO/v1/2005/05/08/A2005128120000.GEO*
#
# Hadoop 1.2.1
# Found 1 items -rw-r--r-- 3 hadoop supergroup 23821329 2014-08-21 14:13 /calvalus/eodata/MODISA_GEO/v1/2005/05/08/A2005128120000.GEO_LAC
#
# Hadoop 2.4
# -rw-r--r--   2 hadoop hadoop   22028066 2014-05-21 17:37 /calvalus/eodata/MODISA_GEO/v1/2005/05/11/A2005131235500.GEO_LAC
#
function from_hdfs {
  local path=$1
  local granule=$2
  echo "testing HDFS ${path}/${granule}.GEO"
  local hadoopLsResult=$(hadoop fs -ls ${path}/${granule}.GEO*)
  if [[ ${hadoopLsResult} ]]; then
    for element in ${hadoopLsResult}; do
      if [[ ${element} =~  ^${path} ]]; then
        local geoPath=${element}
        echo "copy to CWD: ${geoPath} -> ${granule}.GEO"
        hadoop fs -copyToLocal $geoPath ${granule}.GEO
        return 0
      fi
    done
  fi
  return 1
}

inputPath=$1
filename=$(filename ${inputPath})
granulename=$(granulename ${filename})

# test same path
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

if [ -f ${granule}.GEO ]; then
  exit 0;
fi
if from_hdfs $(parentdir ${inputPath}) ${granulename}; then
  exit 0
fi
if from_hdfs /calvalus/eodata/MODISA_GEO/v1/$(datetree ${granulename}) ${granulename}; then
  exit 0
fi
echo "failed to get required GEO file"
exit 1

