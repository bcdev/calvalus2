<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:wps="http://www.opengis.net/wps/1.0.0"
                xmlns:ows="http://www.opengis.net/ows/1.1"
                xmlns:xlink="http://www.w3.org/1999/xlink">
  <xsl:output method="text"/>

  <!-- parameters -->

  <xsl:param name="calvalus.input" />
  <xsl:param name="calvalus.task.id">default-task-id</xsl:param>
  <xsl:param name="calvalus.package.dir">/home/hadoop/opt/seadas-6.3</xsl:param>
  <xsl:variable name="l2gen.envscript">config/seadas.env</xsl:variable>
  <xsl:variable name="l2gen.executable">run/bin/linux_64/l2gen</xsl:variable>
  <xsl:variable name="output" select="/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.output.dir']/wps:Data/wps:Reference/@xlink:href" />

  <!-- variables computed from parameters -->

  <xsl:variable name="calvalus.input.filename" select="tokenize($calvalus.input,'/')[last()]" />

  <xsl:variable name="calvalus.output.filename">
    <xsl:choose>
      <xsl:when test="starts-with($calvalus.input.filename,'MER_')">
        <xsl:value-of select="substring($calvalus.input.filename,1,8)" />
        <xsl:text>2</xsl:text>
        <xsl:value-of select="substring($calvalus.input.filename,10,string-length($calvalus.input.filename)-12)" />
        <xsl:text>.hdf</xsl:text>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$calvalus.input.filename" />
        <xsl:text>.l2.hdf</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <!-- constructs call with env script, executable, input, output, move of temporal output to target dir -->

  <xsl:template match="/">
    <!-- source env file -->
    <xsl:text>set -e; . </xsl:text>
    <xsl:value-of select="$calvalus.package.dir" />
    <xsl:text>/</xsl:text>
    <xsl:value-of select="$l2gen.envscript" />
    <!-- get input -->
    <xsl:text> ; hadoop fs -copyToLocal </xsl:text>
    <xsl:value-of select="$calvalus.input" />
    <xsl:text> . ; </xsl:text>
    <!-- executable -->
    <xsl:value-of select="$calvalus.package.dir" />
    <xsl:text>/</xsl:text>
    <xsl:value-of select="$l2gen.executable" />
    <!-- input file parameter -->
    <xsl:text> ifile=</xsl:text>
    <xsl:value-of select="$calvalus.input.filename" />
    <!-- temporary output file -->
    <xsl:text> ofile=</xsl:text>
    <xsl:value-of select="$calvalus.output.filename" />
    <!-- other parameters -->
    <xsl:apply-templates />
    <!-- move tmp output to (hdfs) destination -->
    <xsl:text> ; hadoop fs -copyFromLocal </xsl:text>
    <xsl:value-of select="$calvalus.output.filename" />
    <xsl:text> </xsl:text>
    <xsl:value-of select="$output" />
    <xsl:text>/</xsl:text>
    <xsl:value-of select="$calvalus.output.filename" />
  </xsl:template>

  <!-- prints out non-calvalus parameter -->

  <xsl:template match="wps:Input[not(starts-with(ows:Identifier,'calvalus.'))]">
    <xsl:text> </xsl:text>
    <xsl:value-of select="ows:Identifier" />
    <xsl:text>=</xsl:text>
    <xsl:value-of select="wps:Data/wps:LiteralData" />
  </xsl:template>

  <!-- catches all other stuff -->

  <xsl:template match="@*|node()" >
    <xsl:apply-templates select="@*|node()"/>
  </xsl:template>

</xsl:stylesheet>
