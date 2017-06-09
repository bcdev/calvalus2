<?xml version="1.0" ?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:template match="property">
        <xsl:value-of select="name"/>
        <xsl:value-of select="value"/>
        <xsl:apply-templates select="source"/>
    </xsl:template>
    <xsl:template match="/">
        <conf>
            <path>
                <xsl:value-of select="conf/path"/>
            </path>
        </conf>
    </xsl:template>

</xsl:stylesheet>