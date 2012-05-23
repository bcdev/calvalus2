<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:wps="http://www.opengis.net/wps/1.0.0"
                xmlns:ows="http://www.opengis.net/ows/1.1"
                xmlns:xlink="http://www.w3.org/1999/xlink">
  <xsl:output method="text"/>

  <!-- parameters -->

  <xsl:param name="calvalus.input" />
  <xsl:param name="calvalus.task.id">default-task-id</xsl:param>
  <xsl:param name="calvalus.package.dir">/home/hadoop/opt/amorgos-4or3</xsl:param>
  <xsl:param name="calvalus.tmp.dir">.</xsl:param>
  <xsl:variable name="amorgos.executable">bin/amorgos.sh</xsl:variable>
  <xsl:variable name="amorgos.executable.3.0">bin/amorgos-3.0.sh</xsl:variable>
  <xsl:variable name="amorgos.reportprogress">bin/amorgos-reportprogress.sh</xsl:variable>

  <!-- variables computed from parameters -->

  <xsl:variable name="calvalus.input.filename" select="tokenize($calvalus.input,'/')[last()]" />
  <xsl:variable name="calvalus.input.year"  select="substring($calvalus.input.filename,15,4)" />
  <xsl:variable name="calvalus.input.month" select="substring($calvalus.input.filename,19,2)" />
  <xsl:variable name="calvalus.input.day"   select="substring($calvalus.input.filename,21,2)" />
  <xsl:variable name="calvalus.input.hour"  select="substring($calvalus.input.filename,24,2)" />
  <xsl:variable name="calvalus.output" select="/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.output.dir']/wps:Data/wps:Reference/@xlink:href" />
  <xsl:variable name="calvalus.catalogue">
    <xsl:choose>
      <xsl:when test="/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.catalogue']/wps:Data/wps:Reference/@xlink:href!=''">
        <xsl:value-of select="/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.catalogue']/wps:Data/wps:Reference/@xlink:href" />
      </xsl:when>
      <xsl:otherwise>hdfs://master00:9000/calvalus/projects/lc/<xsl:value-of select="$calvalus.input.year" />-<xsl:value-of select="$calvalus.input.resolution" />/inventory-l1b/part-r-00000</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <xsl:variable name="calvalus.dem">
    <xsl:choose>
      <xsl:when test="/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.dem']/wps:Data/wps:LiteralData!=''">
        <xsl:value-of select="/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.dem']/wps:Data/wps:LiteralData" />
      </xsl:when>
      <xsl:otherwise>GETASSE30.patched</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="calvalus.input.resolution" select="lower-case(substring($calvalus.input.filename,5,2))" />
  <xsl:variable name="amorgos.split.threshold">
    <xsl:choose>
      <xsl:when test="$calvalus.input.resolution='fr'">10305</xsl:when>
      <xsl:otherwise>41217</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <xsl:variable name="amorgos.tiepoint.lines">
    <xsl:choose>
      <xsl:when test="$calvalus.input.resolution='fr'">64</xsl:when>
      <xsl:otherwise>16</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <xsl:variable name="amorgos.line.time">
    <xsl:choose>
      <xsl:when test="$calvalus.input.resolution='fr'">43997</xsl:when>
      <xsl:otherwise>175988</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <!-- constructs call with env script, executable, input, output, move of temporal output to target dir -->

  <xsl:template match="/">
# switch on job control
set -m

########## function definitions ##########

retrieve_inputs() {
  if [ $retrieval = 0 ]; then
    # retrieve input file
    hadoop fs -get <xsl:value-of select="$calvalus.input" /><xsl:text> </xsl:text><xsl:value-of select="$calvalus.tmp.dir" />/MER_XRX_1P
    export input_version=`head <xsl:value-of select="$calvalus.tmp.dir" />/MER_XRX_1P|awk '{ FS="\"" } /SOFTWARE_VER/ { print $2 }'`
    if [ $input_version = "MERIS/4.10" ]; then
      mv MER_XRX_1P MER_FRX_1P
    fi
    # retrieve att file
    hadoop fs -get hdfs://master00:9000/calvalus/auxiliary/amorgos-3.0/AUX_FRA_AX/<xsl:value-of
              select="$calvalus.input.year" />/AUX_FRA_AXVFOS<xsl:value-of
              select="$calvalus.input.year" /><xsl:value-of
              select="$calvalus.input.month" /><xsl:value-of
              select="$calvalus.input.day" /><xsl:text> </xsl:text><xsl:value-of select="$calvalus.tmp.dir" />/AUX_FRA_AX
    # retrieve orb file, use next day if after 23:00 since it is valid only until 23 minutes after midnight
    if [ "<xsl:value-of select='$calvalus.input.hour'/>" != "23" ]; then
      yearmonthday=<xsl:value-of select="$calvalus.input.year" /><xsl:value-of select="$calvalus.input.month" /><xsl:value-of select="$calvalus.input.day" />
    else
      i=<xsl:value-of select="$calvalus.input.filename"/>
      inputstart=`date +%s -u -d "${i:14:4}-${i:18:2}-${i:20:2} ${i:23:2}:${i:25:2}:${i:27:2}"`
      let nextday="$inputstart + 86400"
      yearmonthday=`date +%Y%m%d -u -d @$nextday`
    fi
#    hadoop fs -get hdfs://master00:9000/calvalus/auxiliary/amorgos-3.0/AUX_FRA_AX/${yearmonthday:0:4}/AUX_FRA_AXVFOS${yearmonthday}<xsl:text> </xsl:text><xsl:value-of select="$calvalus.tmp.dir" />/AUX_FRA_AX

    hadoop fs -get hdfs://master00:9000/calvalus/auxiliary/amorgos-3.0/DOR_VOR_AX/${yearmonthday:0:4}/DOR_VOR_AXVF-P${yearmonthday}<xsl:text> </xsl:text><xsl:value-of select="$calvalus.tmp.dir" />/DOR_VOR_AX
    export retrieval=1
  fi
}

# call_amorgos_and_archive_output amorgos.parameters
call_amorgos_and_archive_output() {
  if [ $input_version = "MERIS/4.10" ]; then
    <xsl:value-of select="$calvalus.package.dir" />/<xsl:value-of select="$amorgos.executable.3.0" /> $1
  else
    <xsl:value-of select="$calvalus.package.dir" />/<xsl:value-of select="$amorgos.executable" /> $1
  fi
  code=$?
  if [ "`cat errors.txt`" != "" ]; then
    ls -ltr
    cat amorgos.parameters
    cat errors.txt
    echo "code = $code"
    code=1
  else
    outputfile=`head -1 MER_??G_1P | cut -d'=' -f2 | sed s/\"//g`
    mv MER_??G_1P $outputfile
    echo "archiving result file $outputfile"
    year=${outputfile:14:4}
    month=${outputfile:18:2}
    day=${outputfile:20:2}
    hadoop fs -put $outputfile <xsl:value-of select="$calvalus.output" />/$year/$month/$day/$outputfile
    hadoop fs -touchz <xsl:value-of select="$calvalus.output" />/$year/$month/$day/${outputfile}.valid
  fi
  return $code
}

function do_process() {
  if [ "$retrieval" = "0" ]; then
    retrieve_inputs
    <xsl:value-of select="$calvalus.package.dir" />/<xsl:value-of select="$amorgos.reportprogress" /> &amp;
  fi
  cat &gt; amorgos.parameters &lt;&lt;EOF
NAME_INPUT_DIR="<xsl:value-of select="$calvalus.tmp.dir" />"
EOF
  if [ $input_version = "MERIS/4.10" ]; then
    cat &gt;&gt; amorgos.parameters &lt;&lt;EOF
NAME_AUX_DIR="<xsl:value-of select="$calvalus.package.dir" />/AuxDir-3.0/"
EOF
  else
    cat &gt;&gt; amorgos.parameters &lt;&lt;EOF
NAME_AUX_DIR="<xsl:value-of select="$calvalus.package.dir" />/AuxDir/"
EOF
  fi
  cat &gt;&gt; amorgos.parameters &lt;&lt;EOF
NAME_DEM_DIR="/home/hadoop/opt/<xsl:value-of select="$calvalus.dem" />/"
NAME_OUTPUT_DIR="<xsl:value-of select="$calvalus.tmp.dir" />"
EOF
  if [ "$1" != "" ]; then
    cat &gt;&gt; amorgos.parameters &lt;&lt;EOF
FIRST_FRAME=$1
FRAME_NUMBER=$2
EOF
  fi
  call_amorgos_and_archive_output amorgos.parameters
}

# lookup_output $expected1
lookup_output() {
  [ "$1" != "" ] &amp;&amp; \
  year=${1:14:4} &amp;&amp; \
  month=${1:18:2} &amp;&amp; \
  day=${1:20:2} &amp;&amp; \
  o=`hadoop fs -ls <xsl:value-of select="$calvalus.output" />/$year/$month/$day/$1 2&gt; /dev/null`
  if [ $? = 0 ]; then
    f=`basename "$o"`
    #f=`echo $o|awk 'BEGIN { FS = "/" } /MER_/ { print $9 }'`
    hadoop fs -touchz <xsl:value-of select="$calvalus.output" />/$year/$month/$day/${f}.valid
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
    let newsec1="$inputstart + ($2 - 1) * <xsl:value-of select="$amorgos.line.time"/> / 1000000"
    let newsec2="$newsec1 - 1"
    let newsec3="$newsec1 + 1"
    newdate1=`date +%Y%m%d_%H%M%S -u -d @$newsec1`
    newdate2=`date +%Y%m%d_%H%M%S -u -d @$newsec2`
    newdate3=`date +%Y%m%d_%H%M%S -u -d @$newsec3`
    let lensec="($3 - 1) * <xsl:value-of select="$amorgos.line.time"/> / 1000000"
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

# cleanup
rm -f *.txt MER_??G_1P MER_??G_1P.lock
# retrieve catalogue file
hadoop fs -get <xsl:value-of select="$calvalus.catalogue" /><xsl:text> </xsl:text><xsl:value-of
          select="$calvalus.tmp.dir" />/catalogue.list
input=`awk '/<xsl:value-of select="$calvalus.input.filename" />/ { print $1 }' <xsl:value-of
          select="$calvalus.tmp.dir" />/catalogue.list`
length=`awk '/<xsl:value-of select="$calvalus.input.filename" />/ { print $4 }' <xsl:value-of
          select="$calvalus.tmp.dir" />/catalogue.list`
start=`awk '/<xsl:value-of select="$calvalus.input.filename" />/ { print $5 }' <xsl:value-of
          select="$calvalus.tmp.dir" />/catalogue.list`
count=`awk '/<xsl:value-of select="$calvalus.input.filename" />/ { print $6 }' <xsl:value-of
          select="$calvalus.tmp.dir" />/catalogue.list`

MAX_LINES=<xsl:value-of select="$amorgos.split.threshold" />
TIEPOINT_LINES=<xsl:value-of select="$amorgos.tiepoint.lines" />
retrieval=0
code=0

if [ "$count" = "0" ]; then
  echo "$input will be dropped"
  exit 0
fi

retrieve_inputs
<xsl:value-of select="$calvalus.package.dir" />/<xsl:value-of select="$amorgos.reportprogress" /> &amp;

if [ "$input" = "" ]; then
  if ! output_exists <xsl:value-of select="$calvalus.input.filename" />; then
    echo "<xsl:value-of select="$calvalus.input.filename" /> not listed, will be processed completely"
    do_process
    code=$?
  fi
else

let a0="($start / $TIEPOINT_LINES) * $TIEPOINT_LINES + 1"
let a1="($start + $count -1 + $TIEPOINT_LINES - 1) / $TIEPOINT_LINES * $TIEPOINT_LINES + 1"
let amorgos_lines="$a1 - $a0 + 1"

echo "ao" $a0
echo "a1" $a1
echo "amorgos_lines" $amorgos_lines

if [ "$amorgos_lines" = "$length" -a $amorgos_lines -le $MAX_LINES ]; then
  if ! output_exists $input 1 $length; then
    echo "$input will be processed completely"
    do_process
    code=$?
  fi
else
  if [ $amorgos_lines -le $MAX_LINES ]; then
    if ! output_exists $input $a0 $amorgos_lines; then
      echo "$input will be processed for one subset"
      do_process $a0 $amorgos_lines
      code=$?
    fi
  else
    if ! output_exists $input $a0 $MAX_LINES; then
      echo "$input will be processed for first subset"
      do_process $a0 $MAX_LINES
      code=$?
    fi
    let ax0="$a0 + $MAX_LINES - 1"
    let ax_lines="$a1 - $a0 - $MAX_LINES + 2"
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

  </xsl:template>

  <!-- catches all other stuff -->

  <xsl:template match="@*|node()" >
    <xsl:apply-templates select="@*|node()"/>
  </xsl:template>

</xsl:stylesheet>
