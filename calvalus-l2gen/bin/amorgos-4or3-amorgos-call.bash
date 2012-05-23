#!/bin/bash
# export amorgos_dem=GETASSE_...
# export l1b_catalogue=hdfs:...
# export valid_markers=true
# amorgos-4or3-amorgos-call.bash hdfs:.../MER_FRS_...N1 hdfs:.../outputs
set -e
set -m

packageDir=`dirname $0`
inputUrl=$1
outputDir=$2

inputFilename=`basename $inputUrl`
year=${inputFilename:14:4}
month=${inputFilename:18:2}
day=${inputFilename:20:2}
hour=${inputFilename:23:2}
resolution=`echo ${inputFilename:4:2}|tr FR fr`

if [ "$amorgos_dem" = "" ]; then
  amorgos_dem=GETASSE_dataset_20120215
fi
if [ "$l1b_catalogue" = "" ]; then
  l1b_catalogue="hdfs://master00:9000/calvalus/projects/lc/$year-$resolution/inventory-l1b/part-r-00000"
fi
if [ "$resolution" = "fr" ]; then
  splitThreshold=10305
  tiepointLines=64
  lineTime=43997
else
  splitThreshold=41217
  tiepointLines=16
  lineTime=175988
fi  

########## function definitions ##########

retrieve_inputs() {
  if [ $retrieval = 0 ]; then
    # retrieve input file
    hadoop fs -get $inputUrl MER_XRX_1P
    export inputVersion=`head MER_XRX_1P|awk '{ FS="\"" } /SOFTWARE_VER/ { print $2 }'`
    if [ $inputVersion = "MERIS/4.10" ]; then
      mv MER_XRX_1P MER_FRX_1P
    fi
    # retrieve att file
    hadoop fs -get hdfs://master00:9000/calvalus/auxiliary/amorgos-3.0/AUX_FRA_AX/$year/AUX_FRA_AXVFOS$year$month$day AUX_FRA_AX
    # retrieve orb file, use next day if after 23:00 since it is valid only until 23 minutes after midnight
    if [ "$hour" != "23" ]; then
      yearmonthday=$year$month$day
    else
      i=$inputFilename
      inputstart=`date +%s -u -d "${i:14:4}-${i:18:2}-${i:20:2} ${i:23:2}:${i:25:2}:${i:27:2}"`
      let nextday="$inputstart + 86400"
      yearmonthday=`date +%Y%m%d -u -d @$nextday`
    fi
#    hadoop fs -get hdfs://master00:9000/calvalus/auxiliary/amorgos-3.0/AUX_FRA_AX/${yearmonthday:0:4}/AUX_FRA_AXVFOS${yearmonthday} AUX_FRA_AX

    hadoop fs -get hdfs://master00:9000/calvalus/auxiliary/amorgos-3.0/DOR_VOR_AX/${yearmonthday:0:4}/DOR_VOR_AXVF-P${yearmonthday} DOR_VOR_AX
    export retrieval=1
  fi
}

# call_amorgos_and_archive_output amorgos.parameters
call_amorgos_and_archive_output() {
  if [ $inputVersion = "MERIS/4.10" ]; then
    $packageDir/bin/amorgos-3.0.sh $1
  else
    $packageDir/bin/amorgos.sh $1
  fi
  code=$?
  if [ "`cat errors.txt`" != "" ]; then
    ls -ltr
    cat amorgos.parameters
    cat errors.txt
    echo "code = $code"
    code=1
  else
    outputFilename=`head -1 MER_??G_1P | cut -d'=' -f2 | sed s/\"//g`
    mv MER_??G_1P $outputFilename
    echo "archiving result file $outputFilename"
    year=${outputFilename:14:4}
    month=${outputFilename:18:2}
    day=${outputFilename:20:2}
    hadoop fs -put $outputFilename $outputDir/$year/$month/$day/$outputFilename
    if [ "$valid_markers" = "true" ]; then
      hadoop fs -touchz $outputDir/$year/$month/$day/${outputFilename}.valid
    fi
  fi
  return $code
}

function do_process() {
  if [ "$retrieval" = "0" ]; then
    retrieve_inputs
    $packageDir/bin/amorgos-reportprogress.sh &
  fi
  cat > amorgos.parameters <<EOF
NAME_INPUT_DIR="."
EOF
  if [ $inputVersion = "MERIS/4.10" ]; then
    cat >> amorgos.parameters <<EOF
NAME_AUX_DIR="$packageDir/AuxDir-3.0/"
EOF
  else
    cat >> amorgos.parameters <<EOF
NAME_AUX_DIR="$packageDir/AuxDir/"
EOF
  fi
  cat >> amorgos.parameters <<EOF
NAME_DEM_DIR="/home/hadoop/opt/$amorgos_dem/"
NAME_OUTPUT_DIR="."
EOF
  if [ "$1" != "" ]; then
    cat >> amorgos.parameters <<EOF
FIRST_FRAME=$1
FRAME_NUMBER=$2
EOF
  fi
  call_amorgos_and_archive_output amorgos.parameters
}

# lookup_output $expected1
lookup_output() {
  [ "$1" != "" ] && \
  year=${1:14:4} && \
  month=${1:18:2} && \
  day=${1:20:2} && \
  o=`hadoop fs -ls $outputDir/$year/$month/$day/$1 2> /dev/null`
  if [ $? = 0 ]; then
    if [ "$valid_markers" = "true" ]; then
      f=`basename "$o"`
      hadoop fs -touchz $outputDir/$year/$month/$day/${f}.valid
    fi
    return 0
  else
    return 1
  fi
}

# if ! output_exists $input $a0 $amorgos_lines
# if ! output_exists $input  # for complete product
output_exists() {
  if [ "$2" = "" ]; then
    expected1="MER_??G_1P${1:10}"
    expected2=
    expected3=
  else
    inputstart=`date +%s -u -d "${1:14:4}-${1:18:2}-${1:20:2} ${1:23:2}:${1:25:2}:${1:27:2}"`
    let newsec1="$inputstart + ($2 - 1) * $lineTime / 1000000"
    let newsec2="$newsec1 - 1"
    let newsec3="$newsec1 + 1"
    newdate1=`date +%Y%m%d_%H%M%S -u -d @$newsec1`
    newdate2=`date +%Y%m%d_%H%M%S -u -d @$newsec2`
    newdate3=`date +%Y%m%d_%H%M%S -u -d @$newsec3`
    let lensec="($3 - 1) * $lineTime / 1000000"
    lenstr=`printf "%08d" $lensec`
    expected1="MER_??G_1P${1:10:4}${newdate1:0:15}_${lenstr}${1:38:24}"
    expected2="MER_??G_1P${1:10:4}${newdate2:0:15}_${lenstr}${1:38:24}"
    expected3="MER_??G_1P${1:10:4}${newdate3:0:15}_${lenstr}${1:38:24}"
  fi
  if lookup_output $expected1; then
    echo "skipping $1, $expected1 exists"
    return 0
  elif lookup_output $expected2; then
    echo "skipping $1, $expected2 exists"
    return 0
  elif lookup_output $expected3; then
    echo "skipping $1, $expected3 exists"
    return 0
  else
    return 1
  fi
}

########## end of function definitions ##########

echo "handling input $inputFilename"

# cleanup
rm -f *.txt MER_??G_1P MER_??G_1P.lock
# retrieve catalogue file
if [ "$l1b_catalogue" != "" ]; then
  echo "retrieving catalogue $l1b_catalogue"
  hadoop fs -get $l1b_catalogue catalogue.list
else
  touch catalogue.list
fi
input=`awk "/$inputFilename/"' { print $1 }' catalogue.list`
length=`awk "/$inputFilename/"' { print $4 }' catalogue.list`
start=`awk "/$inputFilename/"' { print $5 }' catalogue.list`
count=`awk "/$inputFilename/"' { print $6 }' catalogue.list`

retrieval=0
code=0

if [ "$count" = "0" ]; then
  echo "$input will be dropped"
  exit 0
fi

if [ "$input" = "" ]; then
  if ! output_exists $inputFilename; then
    echo "$inputFilename not listed, will be processed completely"
    do_process
    code=$?
  fi
else

let a0="($start / $tiepointLines) * $tiepointLines + 1"
let a1="($start + $count -1 + $tiepointLines - 1) / $tiepointLines * $tiepointLines + 1"
let amorgos_lines="$a1 - $a0 + 1"

echo "ao" $a0
echo "a1" $a1
echo "amorgos_lines" $amorgos_lines

if [ "$amorgos_lines" = "$length" -a $amorgos_lines -le $splitThreshold ]; then
  if ! output_exists $input 1 $length; then
    echo "$input will be processed completely"
    do_process
    code=$?
  fi
else
  if [ $amorgos_lines -le $splitThreshold ]; then
    if ! output_exists $input $a0 $amorgos_lines; then
      echo "$input will be processed for one subset"
      do_process $a0 $amorgos_lines
      code=$?
    fi
  else
    if ! output_exists $input $a0 $splitThreshold; then
      echo "$input will be processed for first subset"
      do_process $a0 $splitThreshold
      code=$?
    fi
    let ax0="$a0 + $splitThreshold - 1"
    let ax_lines="$a1 - $a0 - $splitThreshold + 2"
    if ! output_exists $input $ax0 $ax_lines; then
      echo "$input will be processed for second subset"
      do_process $ax0 $ax_lines
      code=$? || $code
    fi
  fi
fi

fi

if [ "$retrieval" != "0" ]; then
  kill %1
fi
exit $code
