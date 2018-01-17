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
                <xsl:if test="name='calvalus.productionType'">
                    <workflowType>
                        <xsl:value-of select="value"/>
                    </workflowType>
                </xsl:if>

                <xsl:if test="name='calvalus.l2.operator'">
                    <dataProcessorUsed>
                        <xsl:value-of select="value"/>
                    </dataProcessorUsed>
                </xsl:if>

                <xsl:if test="name='mapreduce.map.cpu.vcores'">
                    <configuredCpuCores>
                        <xsl:value-of select="value"/>
                    </configuredCpuCores>
                </xsl:if>

                <xsl:if test="name='mapreduce.map.memory.mb'">
                    <configuredRam>
                        <xsl:value-of select="value"/>
                    </configuredRam>
                </xsl:if>

                <xsl:if test="name='calvalus.input.collectionName'">
                    <inputCollectionName>
                        <xsl:value-of select="value"/>
                    </inputCollectionName>
                </xsl:if>

                <xsl:if test="name='calvalus.input.productType'">
                    <inputType>
                        <xsl:value-of select="value"/>
                    </inputType>
                </xsl:if>

                <xsl:if test="name='calvalus.system.name'">
                    <systemName>
                        <xsl:value-of select="value"/>
                    </systemName>
                </xsl:if>

                <xsl:if test="name='calvalus.output.productType'">
                    <outputType>
                        <xsl:value-of select="value"/>
                    </outputType>
                </xsl:if>

                <xsl:choose>
                    <xsl:when test="name='calvalus.input.pathPatterns'">
                        <inputPath>
                            <xsl:value-of select="value"/>
                        </inputPath>
                    </xsl:when>
                    <xsl:when test="name='calvalus.input.geoInventory'">
                        <inputPath>
                            <xsl:value-of select="value"/>
                        </inputPath>
                    </xsl:when>
                </xsl:choose>
            </xsl:for-each>
        </conf>
    </xsl:template>

</xsl:stylesheet>