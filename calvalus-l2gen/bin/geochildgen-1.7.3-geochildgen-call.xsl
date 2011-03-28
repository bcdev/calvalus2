<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:wps="http://www.opengis.net/wps/1.0.0"
                xmlns:ows="http://www.opengis.net/ows/1.1"
                xmlns:xlink="http://www.w3.org/1999/xlink">
  <xsl:output method="text"/>

  <!-- parameters -->

  <xsl:param name="calvalus.input" />
  <xsl:param name="calvalus.task.id">default-task-id</xsl:param>
  <xsl:param name="calvalus.package.dir">/home/hadoop/opt/geochildgen-1.7.3</xsl:param>
  <xsl:param name="calvalus.archive.mount">/mnt/hdfs</xsl:param>
  <xsl:param name="calvalus.tmp.dir">/home/hadoop/tmp/<xsl:value-of select="$calvalus.task.id" /></xsl:param>
  <xsl:variable name="geochildgen.executable">geochildgen.sh</xsl:variable>

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
    <!-- create tmp dir -->
    <xsl:text>mkdir -p </xsl:text>
    <xsl:value-of select="$calvalus.tmp.dir" />
    <xsl:text> ; </xsl:text>
    <!-- create output dir -->
    <xsl:text>mkdir -p </xsl:text>
    <xsl:value-of select="$calvalus.output.physical" />
    <xsl:text> ; </xsl:text>
    <!-- call geochildgen per subset -->
    <xsl:apply-templates />
    <!-- cleanup -->
    <xsl:text>rm -r </xsl:text>
    <xsl:value-of select="$calvalus.tmp.dir" />
  </xsl:template>

  <!-- prints out one call of geochildgen and the output transfer command -->

  <xsl:template match="wps:Input[ows:Identifier='subset']">
    <!-- cleanup -->
    <xsl:text>rm -f </xsl:text>
    <xsl:value-of select="$calvalus.tmp.dir" />
    <xsl:text>/*.N1 ; </xsl:text>
    <!-- write properties file -->
    <xsl:result-document href="{$calvalus.tmp.dir}/{wps:Data/wps:ComplexData/identifier}.properties">
      <xsl:text>geometry[0] = </xsl:text>
      <xsl:value-of select="wps:Data/wps:ComplexData/coverage" />
    </xsl:result-document>
    <!-- executable -->
    <xsl:value-of select="$calvalus.package.dir" />
    <xsl:text>/</xsl:text>
    <xsl:value-of select="$geochildgen.executable" />
    <!-- parameters -->
    <xsl:text> -g </xsl:text>
    <xsl:value-of select="$calvalus.tmp.dir" />
    <xsl:text>/</xsl:text>
    <xsl:value-of select="wps:Data/wps:ComplexData/identifier" />
    <xsl:text>.properties -c -o </xsl:text>
    <xsl:value-of select="$calvalus.tmp.dir" />
    <xsl:text> </xsl:text>
    <xsl:value-of select="$calvalus.input.physical" />
    <xsl:text> ; </xsl:text>
    <!-- move tmp output to (hdfs) destination -->
    <xsl:text>if test -e </xsl:text>
    <xsl:value-of select="$calvalus.tmp.dir" />
    <xsl:text>/*.N1 ; then mkdir -p </xsl:text>
    <xsl:value-of select="$calvalus.output.physical" />
    <xsl:text>/</xsl:text>
    <xsl:value-of select="wps:Data/wps:ComplexData/identifier" />
    <xsl:text>/</xsl:text>
    <xsl:value-of select="$calvalus.input.year" />
    <xsl:text>/</xsl:text>
    <xsl:value-of select="$calvalus.input.month" />
    <xsl:text>/</xsl:text>
    <xsl:value-of select="$calvalus.input.day" />
    <xsl:text> ; mv </xsl:text>
    <xsl:value-of select="$calvalus.tmp.dir" />
    <xsl:text>/*.N1 </xsl:text>
    <xsl:value-of select="$calvalus.output.physical" />
    <xsl:text>/</xsl:text>
    <xsl:value-of select="wps:Data/wps:ComplexData/identifier" />
    <xsl:text>/</xsl:text>
    <xsl:value-of select="$calvalus.input.year" />
    <xsl:text>/</xsl:text>
    <xsl:value-of select="$calvalus.input.month" />
    <xsl:text>/</xsl:text>
    <xsl:value-of select="$calvalus.input.day" />
    <xsl:text>/</xsl:text>
    <xsl:value-of select="substring-before($calvalus.input.filename,'.N1')" />
    <xsl:text>-</xsl:text>
    <xsl:value-of select="wps:Data/wps:ComplexData/identifier" />
    <xsl:text>.N1 ; fi ; </xsl:text>
  </xsl:template>

  <!-- catches all other stuff -->

  <xsl:template match="@*|node()" >
    <xsl:apply-templates select="@*|node()"/>
  </xsl:template>

</xsl:stylesheet>
