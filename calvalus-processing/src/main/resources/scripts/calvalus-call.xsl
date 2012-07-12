<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:wps="http://www.opengis.net/wps/1.0.0"
                xmlns:ows="http://www.opengis.net/ows/1.1"
                xmlns:xlink="http://www.w3.org/1999/xlink">
  <xsl:output method="text"/>

  <xsl:param name="calvalus.input" />
  <xsl:param name="calvalus.script" />
  <xsl:param name="calvalus.package.dir"/>

  <xsl:variable name="calvalus.output.dir" select="/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.output.dir']/wps:Data/wps:Reference/@xlink:href" />

  <xsl:template match="/">
export calvalus_package_dir='<xsl:value-of select="$calvalus.package.dir" />'<xsl:text>
</xsl:text>

<xsl:apply-templates select="wps:Execute" />
<xsl:value-of select="$calvalus.script" /><xsl:text> </xsl:text><xsl:value-of select="$calvalus.input" /><xsl:text> </xsl:text><xsl:value-of select="$calvalus.output.dir" /><xsl:text>
</xsl:text>
  </xsl:template>

  <!-- translates WPS parameters into shell environment variables -->

  <xsl:template match="wps:Input[substring(ows:Identifier,1,9) != 'calvalus.']">
export <xsl:value-of select="translate(ows:Identifier,'.','_')" />='<xsl:value-of select='wps:Data/wps:LiteralData' />'<xsl:text>
</xsl:text>
  </xsl:template>

  <!-- catches all other stuff -->

  <xsl:template match="@*|node()" >
    <xsl:apply-templates select="@*|node()"/>
  </xsl:template>

</xsl:stylesheet>
