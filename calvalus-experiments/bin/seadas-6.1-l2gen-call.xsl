<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:wps="http://www.opengis.net/wps/1.0.0"
                xmlns:ows="http://www.opengis.net/ows/1.1"
                xmlns:xlink="http://www.w3.org/1999/xlink">
  <xsl:output method="text"/>

  <xsl:param name="calvalus.input" />

  <xsl:variable name="calvalus.package.root">/home/hadoop/calvalus</xsl:variable>
  <xsl:variable name="calvalus.data.root">/mnt/hdfs</xsl:variable>
  <xsl:variable name="l2gen.executable">seadas-6.1/bin/l2gen</xsl:variable>

  <xsl:variable name="calvalus.input.filename" select="tokenize($calvalus.input,'/')[last()]" />
  
  <xsl:template match="/">
    <xsl:value-of select="$calvalus.package.root" />
    <xsl:text>/</xsl:text>
    <xsl:value-of select="$l2gen.executable" />

    <xsl:text> ifile=</xsl:text>
    <xsl:value-of select="$calvalus.data.root" />
    <xsl:value-of select="substring-after($calvalus.input,'hdfs:')" />

    <xsl:apply-templates />
  </xsl:template>

  <xsl:template match="wps:Input[starts-with(ows:Identifier,'calvalus.output.dir')]">
    <xsl:text> ofile=</xsl:text>
    <xsl:value-of select="$calvalus.data.root" />
    <xsl:text>/</xsl:text>
    <xsl:value-of select="substring-after(substring-after(wps:Data/wps:Reference/@xlink:href,'hdfs://'),'/')" />
    <xsl:text>/</xsl:text>
    <xsl:choose>
      <xsl:when test="starts-with($calvalus.input.filename,'MER_')">
        <xsl:value-of select="substring($calvalus.input.filename,1,8)" />
        <xsl:text>2</xsl:text>
        <xsl:value-of select="substring($calvalus.input.filename,10)" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$calvalus.input.filename" />
        <xsl:text>.l2.hdf</xsl:text>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="wps:Input[not(starts-with(ows:Identifier,'calvalus.'))]">
    <xsl:text> </xsl:text>
    <xsl:value-of select="ows:Identifier" />
    <xsl:text>=</xsl:text>
    <xsl:value-of select="wps:Data/wps:LiteralData" />
  </xsl:template>

  <xsl:template match="@*|node()" >
    <xsl:apply-templates select="@*|node()"/>
  </xsl:template>

</xsl:stylesheet>
