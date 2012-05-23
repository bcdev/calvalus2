<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:wps="http://www.opengis.net/wps/1.0.0"
                xmlns:ows="http://www.opengis.net/ows/1.1"
                xmlns:xlink="http://www.w3.org/1999/xlink">
  <xsl:output method="text"/>

  <!-- parameters -->

  <xsl:param name="calvalus.input" />
  <xsl:param name="calvalus.package.dir">/home/hadoop/opt/geochildgen-1.7.5</xsl:param>
  <xsl:param name="calvalus.tmp.dir">.</xsl:param>
  <xsl:variable name="geochildgen.executable">geochildgen.sh</xsl:variable>
  <xsl:variable name="geochildgen.reportprogress">geochildgen-reportprogress.sh</xsl:variable>

  <!-- variables computed from parameters -->

  <xsl:variable name="calvalus.input.filename" select="tokenize($calvalus.input,'/')[last()]" />

  <xsl:variable name="calvalus.input.year"  select="substring($calvalus.input.filename,15,4)" />
  <xsl:variable name="calvalus.input.month" select="substring($calvalus.input.filename,19,2)" />
  <xsl:variable name="calvalus.input.day"   select="substring($calvalus.input.filename,21,2)" />

  <xsl:variable name="calvalus.output" select="/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.output.dir']/wps:Data/wps:Reference/@xlink:href" />

  <!-- constructs call with env script, executable, input, output, move of temporal output to target dir -->

  <xsl:template match="/">
# switch on job control
set -m
# create output dir and tmp dir
mkdir -p <xsl:value-of select="$calvalus.tmp.dir" />
cd <xsl:value-of select="$calvalus.tmp.dir" />
# start concurrent status reporting job
<xsl:value-of select="$calvalus.package.dir" />/<xsl:value-of select="$geochildgen.reportprogress" />  &amp;
# call geochildgen per subset
mkdir <xsl:value-of select="$calvalus.tmp.dir" />/input
hadoop fs -get <xsl:value-of select="$calvalus.input" /><xsl:text> </xsl:text><xsl:value-of select="$calvalus.tmp.dir" />/input/<xsl:value-of select="$calvalus.input.filename" />
result=0
<xsl:apply-templates />
# stop concurrent status reporting job
kill %1
test $result = 0
  </xsl:template>

  <!-- prints out one call of geochildgen and the output transfer command -->

  <xsl:template match="wps:Input[ows:Identifier='subset']">
    <!-- write properties file -->
    <xsl:result-document href="{$calvalus.tmp.dir}/{wps:Data/wps:ComplexData/identifier}.properties">
      <xsl:text>geometry[0] = </xsl:text>
      <xsl:value-of select="wps:Data/wps:ComplexData/coverage" />
    </xsl:result-document>
# cleanup
rm -f <xsl:value-of select="$calvalus.tmp.dir" />/*.N1
# executable
if <xsl:value-of select="$calvalus.package.dir" />/<xsl:value-of select="$geochildgen.executable" /> \
  -g <xsl:value-of select="$calvalus.tmp.dir" />/<xsl:value-of select="wps:Data/wps:ComplexData/identifier" />.properties \
  -c \
  -m \
  -o <xsl:value-of select="$calvalus.tmp.dir" /> \
  <xsl:value-of select="$calvalus.tmp.dir" />/input/<xsl:value-of select="$calvalus.input.filename" />
then
  # move tmp output to (hdfs) destination
  if test -e <xsl:value-of select="$calvalus.tmp.dir" />/*.N1
  then
    # name of result file
    outputfilename=`head -1 *.N1 | cut -d'=' -f2 | sed s/\"//g`
    hadoop fs -put *.N1 <xsl:value-of select="$calvalus.output" />/<xsl:value-of select="wps:Data/wps:ComplexData/identifier" />/<xsl:value-of select="$calvalus.input.year" />/<xsl:value-of select="$calvalus.input.month" />/<xsl:value-of select="$calvalus.input.day" />/$outputfilename
  fi
else
  echo "<xsl:value-of select="wps:Data/wps:ComplexData/identifier" /> of <xsl:value-of select="$calvalus.input" /> processing failed"
  result=1
fi
  </xsl:template>

  <!-- catches all other stuff -->

  <xsl:template match="@*|node()" >
    <xsl:apply-templates select="@*|node()"/>
  </xsl:template>

</xsl:stylesheet>
