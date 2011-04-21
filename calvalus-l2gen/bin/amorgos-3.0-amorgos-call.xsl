<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:wps="http://www.opengis.net/wps/1.0.0"
                xmlns:ows="http://www.opengis.net/ows/1.1"
                xmlns:xlink="http://www.w3.org/1999/xlink">
  <xsl:output method="text"/>

  <!-- parameters -->

  <xsl:param name="calvalus.input" />
  <xsl:param name="calvalus.task.id">default-task-id</xsl:param>
  <xsl:param name="calvalus.package.dir">/home/hadoop/opt/amorgos-3.0</xsl:param>
  <xsl:param name="calvalus.archive.mount">/mnt/hdfs</xsl:param>
  <xsl:param name="calvalus.tmp.dir">/home/hadoop/tmp/<xsl:value-of select="$calvalus.task.id" /></xsl:param>
  <xsl:variable name="amorgos.executable">bin/amorgos.sh</xsl:variable>
  <xsl:variable name="amorgos.reportprogress">bin/amorgos-reportprogress.sh</xsl:variable>

  <xsl:variable name="newline"><xsl:text>
</xsl:text></xsl:variable>

  <!-- variables computed from parameters -->

  <xsl:variable name="calvalus.input.filename" select="tokenize($calvalus.input,'/')[last()]" />

  <xsl:variable name="calvalus.input.year"  select="substring($calvalus.input.filename,15,4)" />
  <xsl:variable name="calvalus.input.month" select="substring($calvalus.input.filename,19,2)" />
  <xsl:variable name="calvalus.input.day"   select="substring($calvalus.input.filename,21,2)" />

  <xsl:variable name="calvalus.output" select="/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.output.dir']/wps:Data/wps:Reference/@xlink:href" />

  <xsl:variable name="calvalus.input.physical">
    <xsl:choose>
      <xsl:when test="starts-with($calvalus.input,'hdfs:')">
        <xsl:value-of select="$calvalus.archive.mount" />
        <xsl:text>/</xsl:text>
        <xsl:value-of select="substring-after(substring-after($calvalus.input,'hdfs://'),'/')" />
      </xsl:when>
      <xsl:when test="starts-with($calvalus.input,'file://')">
        <xsl:value-of select="substring-after($calvalus.input,'file://')" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$calvalus.input" />
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <!-- sets variable calvalus.output.physical to final output dir -->

  <xsl:variable name="calvalus.output.physical">
    <xsl:variable name="output" select="/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.output.dir']/wps:Data/wps:Reference/@xlink:href" />
    <xsl:choose>
      <xsl:when test="starts-with($output,'hdfs:')">
        <xsl:value-of select="$calvalus.archive.mount" />
        <xsl:text>/</xsl:text>
        <xsl:value-of select="substring-after(substring-after($output,'hdfs://'),'/')" />
      </xsl:when>
      <xsl:when test="starts-with($output,'file://')">
        <xsl:value-of select="substring-after($output,'file://')" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$output" />
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <!-- constructs call with env script, executable, input, output, move of temporal output to target dir -->

  <xsl:template match="/">
      <!-- write parameter file -->
      <xsl:result-document href="{$calvalus.tmp.dir}/amorgos.parameters">
          NAME_INPUT_DIR="<xsl:value-of select="$calvalus.tmp.dir" />"
          NAME_AUX_DIR="<xsl:value-of select="$calvalus.package.dir" />/AuxDir/"
          NAME_DEM_DIR="<xsl:value-of select="$calvalus.archive.mount" />/calvalus/auxiliary/amorgos-3.0/GETASSE30/"
          NAME_OUTPUT_DIR="<xsl:value-of select="$calvalus.tmp.dir" />"
      </xsl:result-document>
# switch on job control
set -m
# create tmp dir and change into it
mkdir -p <xsl:value-of select="$calvalus.tmp.dir" />
cd <xsl:value-of select="$calvalus.tmp.dir" />
# list parameter file
cat amorgos.parameters
# cleanup
rm -f *.txt MER_FRG_1P MER_FRG_1P.lock
# start concurrent status reporting job
<xsl:value-of select="$calvalus.package.dir" />/<xsl:value-of select="$amorgos.reportprogress" />  &amp;
# link input file
###ln -sf <xsl:value-of select="$calvalus.input.physical" /> MER_FRX_1P
hadoop fs -get <xsl:value-of select="$calvalus.input" /><xsl:text> </xsl:text><xsl:value-of select="$calvalus.tmp.dir" />/MER_FRX_1P
# link att file
#ln -sf <xsl:value-of
          select="$calvalus.archive.mount" />/calvalus/auxiliary/amorgos-3.0/AUX_FRA_AX/<xsl:value-of
          select="$calvalus.input.year" />/AUX_FRA_AXVFOS<xsl:value-of
          select="$calvalus.input.year" /><xsl:value-of
          select="$calvalus.input.month" /><xsl:value-of
          select="$calvalus.input.day" /> AUX_FRA_AX
hadoop fs -get hdfs://cvmaster00:9000/calvalus/auxiliary/amorgos-3.0/AUX_FRA_AX/<xsl:value-of
          select="$calvalus.input.year" />/AUX_FRA_AXVFOS<xsl:value-of
          select="$calvalus.input.year" /><xsl:value-of
          select="$calvalus.input.month" /><xsl:value-of
          select="$calvalus.input.day" /><xsl:text> </xsl:text><xsl:value-of select="$calvalus.tmp.dir" />/AUX_FRA_AX
# link orb file
#ln -sf <xsl:value-of
          select="$calvalus.archive.mount" />/calvalus/auxiliary/amorgos-3.0/DOR_VOR_AX/<xsl:value-of
          select="$calvalus.input.year" />/DOR_VOR_AXVF-P<xsl:value-of
          select="$calvalus.input.year" /><xsl:value-of
          select="$calvalus.input.month" /><xsl:value-of
          select="$calvalus.input.day" /> DOR_VOR_AX
hadoop fs -get hdfs://cvmaster00:9000/calvalus/auxiliary/amorgos-3.0/DOR_VOR_AX/<xsl:value-of
          select="$calvalus.input.year" />/DOR_VOR_AXVF-P<xsl:value-of
          select="$calvalus.input.year" /><xsl:value-of
          select="$calvalus.input.month" /><xsl:value-of
          select="$calvalus.input.day" /><xsl:text> </xsl:text><xsl:value-of select="$calvalus.tmp.dir" />/DOR_VOR_AX
# call executable
<xsl:value-of select="$calvalus.package.dir" />/<xsl:value-of select="$amorgos.executable" /> amorgos.parameters
# stop concurrent status reporting job
kill %1
# rename result file
outputfile=`head -1 MER_FSG_1P | cut -d'=' -f2 | sed s/\"//g`
mv MER_FSG_1P $outputfile
# move tmp output to (hdfs) destination
if test -e <xsl:value-of select="$calvalus.tmp.dir" />/*.N1 ; then
#  mkdir -p <xsl:value-of
          select="$calvalus.output.physical" />/<xsl:value-of
          select="$calvalus.input.year" />/<xsl:value-of
          select="$calvalus.input.month" />/<xsl:value-of
          select="$calvalus.input.day" />
#  mv <xsl:value-of select="$calvalus.tmp.dir" />/*.N1 <xsl:value-of
          select="$calvalus.output.physical" />/<xsl:value-of
          select="$calvalus.input.year" />/<xsl:value-of
          select="$calvalus.input.month" />/<xsl:value-of
          select="$calvalus.input.day" />/
  hadoop fs -put $outputfile <xsl:value-of select="$calvalus.output" />/<xsl:value-of
          select="$calvalus.input.year" />/<xsl:value-of
          select="$calvalus.input.month" />/<xsl:value-of
          select="$calvalus.input.day" />/$outputfile
  rm -f $outputfile
fi
ls -ltr ; cat errors.txt ; cd
# cleanup
rm -r <xsl:value-of select="$calvalus.tmp.dir" />
#
date
test -f <xsl:value-of select="$calvalus.output.physical" />/<xsl:value-of
          select="$calvalus.input.year" />/<xsl:value-of
          select="$calvalus.input.month" />/<xsl:value-of
          select="$calvalus.input.day" />/$outputfile
  </xsl:template>


  <!-- catches all other stuff -->

  <xsl:template match="@*|node()" >
    <xsl:apply-templates select="@*|node()"/>
  </xsl:template>

</xsl:stylesheet>
