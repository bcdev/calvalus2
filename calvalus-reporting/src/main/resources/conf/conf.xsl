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
            <xsl:for-each select="conf/property">
                <xsl:if test="name='calvalus.l2.operator'">
                    <br/>
                    <processType>
                        <xsl:value-of select="value"/>
                    </processType>
                </xsl:if>
                <xsl:if test="name='mapreduce.job.name'">
                    <jobName>
                        <xsl:value-of select="value"/>
                    </jobName>
                </xsl:if>
                <xsl:if test="name='calvalus.wps.remote.user'">
                    <remoteUser>
                        <xsl:value-of select="value"/>
                    </remoteUser>
                </xsl:if>
                <xsl:if test="name='calvalus.wps.remote.ref'">
                    <remoteRef>
                        <xsl:value-of select="value"/>
                    </remoteRef>
                </xsl:if>
                <xsl:if test="name='calvalus.output.dir'">
                    <outputDir>
                        <xsl:value-of select="value"/>
                    </outputDir>
                </xsl:if>
            </xsl:for-each>
        </conf>
    </xsl:template>

</xsl:stylesheet>