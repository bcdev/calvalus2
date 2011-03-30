<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:wps="http://www.opengis.net/wps/1.0.0"
                xmlns:ows="http://www.opengis.net/ows/1.1"
                xmlns:xlink="http://www.w3.org/1999/xlink">
  <xsl:output method="text"/>

  <!-- parameters -->

  <xsl:param name="calvalus.input" />
  <xsl:param name="calvalus.task.id">default-task-id</xsl:param>
  <xsl:param name="calvalus.package.dir">/home/hadoop/opt/beam-bin-4.9</xsl:param>
  <xsl:param name="calvalus.archive.mount">/mnt/hdfs</xsl:param>
  <xsl:param name="calvalus.tmp.dir">/home/hadoop/tmp/<xsl:value-of select="$calvalus.task.id" /></xsl:param>
  <xsl:variable name="pconvert.executable">bin/pconvert.sh</xsl:variable>

  <!-- variables computed from parameters -->

  <xsl:variable name="calvalus.input.filename" select="tokenize($calvalus.input,'/')[last()]" />

  <xsl:variable name="calvalus.input.year"  select="substring($calvalus.input.filename,15,4)" />
  <xsl:variable name="calvalus.input.month" select="substring($calvalus.input.filename,19,2)" />
  <xsl:variable name="calvalus.input.day"   select="substring($calvalus.input.filename,21,2)" />

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
# create output and tmp dir and change into tmp dir
mkdir -p <xsl:value-of select="$calvalus.output.physical" />
mkdir -p <xsl:value-of select="$calvalus.tmp.dir" />
cd <xsl:value-of select="$calvalus.tmp.dir" />
# call executable
<xsl:value-of select="$calvalus.package.dir" />/<xsl:value-of select="$pconvert.executable" /> <xsl:apply-templates /> -o <xsl:value-of select="$calvalus.output.physical" /><xsl:text> </xsl:text><xsl:value-of select="$calvalus.input.physical" />
# cleanup
cd ..
rm -r <xsl:value-of select="$calvalus.tmp.dir" />
#
date
test -e <xsl:value-of select="$calvalus.output.physical" />/<xsl:value-of select="substring-before($calvalus.input.filename,'.N1')" />.png
  </xsl:template>

  <!-- prints out non-calvalus parameter -->

  <xsl:template match="wps:Input[not(starts-with(ows:Identifier,'calvalus.'))]">
    <xsl:text> -</xsl:text>
    <xsl:value-of select="ows:Identifier" />
    <xsl:text> </xsl:text>
    <xsl:value-of select="wps:Data/wps:LiteralData" />
  </xsl:template>

  <!-- catches all other stuff -->

  <xsl:template match="@*|node()" >
    <xsl:apply-templates select="@*|node()"/>
  </xsl:template>

</xsl:stylesheet>
