<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:wps="http://www.opengis.net/wps/1.0.0"
                xmlns:ows="http://www.opengis.net/ows/1.1"
                xmlns:xlink="http://www.w3.org/1999/xlink">
  <xsl:output method="text"/>

  <!-- parameters -->

  <xsl:param name="calvalus.input" />
  <xsl:param name="calvalus.task.id">default-task-id</xsl:param>
  <xsl:param name="calvalus.package.dir">/home/hadoop/opt/geochildgen-1.7.5</xsl:param>
  <xsl:param name="calvalus.tmp.dir">.</xsl:param>
  <xsl:variable name="childgen.executable">childgen.sh</xsl:variable>
  <xsl:variable name="geochildgen.reportprogress">geochildgen-reportprogress.sh</xsl:variable>

  <!-- variables computed from parameters -->

  <xsl:variable name="calvalus.input.filename" select="tokenize($calvalus.input,'/')[last()]" />
  <xsl:variable name="calvalus.input.year"  select="substring($calvalus.input.filename,15,4)" />
  <xsl:variable name="calvalus.input.month" select="substring($calvalus.input.filename,19,2)" />
  <xsl:variable name="calvalus.input.day"   select="substring($calvalus.input.filename,21,2)" />
  <xsl:variable name="calvalus.input.duration" select="substring($calvalus.input.filename,31,8)" />
  <xsl:variable name="calvalus.output" select="/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.output.dir']/wps:Data/wps:Reference/@xlink:href" />

  <!-- constructs call with env script, executable, input, output, move of temporal output to target dir -->

  <xsl:template match="/">
# switch on job control
set -m
#
export PATH=/usr/lib/jvm/default-java/bin:$PATH
# code=`call_childgen_and_archive_output $start1 $count1 $expectedoutput
call_childgen_and_archive_output() {
  input=<xsl:value-of select="$calvalus.input.filename" />
  let stop="$1 + $2 - 1 - 2"
  echo "processing target $3"
  <xsl:value-of select="$calvalus.package.dir" />/<xsl:value-of select="$childgen.executable" /> ${input}.orig . $1 $stop ${input:11:3} ${input:55:4}
  code=$?
  if [ "$code" != 0 ]; then
    ls -ltr
    echo "code = $code"
  else
    outputfile=`ls MER*.N1`
    hadoop fs -put $outputfile <xsl:value-of select="$calvalus.output" />/<xsl:value-of
           select="$calvalus.input.year" />/<xsl:value-of
           select="$calvalus.input.month" />/<xsl:value-of
           select="$calvalus.input.day" />/$outputfile
    rm $outputfile
  fi
  return $code
}
#
# create output dir and tmp dir
mkdir -p <xsl:value-of select="$calvalus.tmp.dir" />
cd <xsl:value-of select="$calvalus.tmp.dir" />
<xsl:text>
</xsl:text>
<xsl:value-of select="$calvalus.package.dir" />/<xsl:value-of select="$geochildgen.reportprogress" />  &amp;
hadoop fs -get <xsl:value-of select="$calvalus.input" /><xsl:text> \
    </xsl:text><xsl:value-of select="$calvalus.tmp.dir" />/<xsl:value-of select="$calvalus.input.filename" />.orig
seconds=<xsl:value-of select="$calvalus.input.duration" />
code=1
let lines="( 1$seconds - 100000000) * 1000000 / 175988 / 16 * 16 + 1 - 1024 - 512"
start=1
while [ 1 ]; do
  if [ $start -ge $lines ]; then
    call_childgen_and_archive_output $start 3072 $start-end
    if [ $? = 0 ]; then code=0; fi
    break
  fi
  call_childgen_and_archive_output $start 1024 $start-1024
  if [ $? = 0 ]; then code=0; fi
  let start="$start + 1024"
done
kill %1
exit $code
  </xsl:template>

  <!-- catches all other stuff -->

  <xsl:template match="@*|node()" >
    <xsl:apply-templates select="@*|node()"/>
  </xsl:template>

</xsl:stylesheet>
