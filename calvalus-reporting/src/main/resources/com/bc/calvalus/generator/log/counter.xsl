<?xml version="1.0" ?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

    <xsl:template match="/">
        <jobCounters>
            <id>
                <xsl:value-of select="jobCounters/id"/>
            </id>
            <counterGroup>
                <counterGroupName>
                    <xsl:value-of select="jobCounters/counterGroup/counterGroupName"/>
                </counterGroupName>

                <xsl:for-each select="jobCounters/counterGroup/counter">
                    <xsl:choose>
                        <xsl:when test="name='FILE_BYTES_READ'">
                            <counter>
                                <name>
                                    <xsl:value-of select="name"/>
                                </name>
                                <totalCounterValue>
                                    <xsl:value-of select="totalCounterValue"/>
                                </totalCounterValue>
                                <mapCounterValue>
                                    <xsl:value-of select="mapCounterValue"/>
                                </mapCounterValue>
                                <reduceCounterValue>
                                    <xsl:value-of select="reduceCounterValue"/>
                                </reduceCounterValue>
                            </counter>
                        </xsl:when>
                        <xsl:when test="name='FILE_BYTES_WRITTEN'">
                            <counter>
                                <name>
                                    <xsl:value-of select="name"/>
                                </name>
                                <totalCounterValue>
                                    <xsl:value-of select="totalCounterValue"/>
                                </totalCounterValue>
                                <mapCounterValue>
                                    <xsl:value-of select="mapCounterValue"/>
                                </mapCounterValue>
                                <reduceCounterValue>
                                    <xsl:value-of select="reduceCounterValue"/>
                                </reduceCounterValue>
                            </counter>

                        </xsl:when>
                        <xsl:when test="name='HDFS_BYTES_READ'">
                            <counter>
                                <name>
                                    <xsl:value-of select="name"/>
                                </name>
                                <totalCounterValue>
                                    <xsl:value-of select="totalCounterValue"/>
                                </totalCounterValue>
                                <mapCounterValue>
                                    <xsl:value-of select="mapCounterValue"/>
                                </mapCounterValue>
                                <reduceCounterValue>
                                    <xsl:value-of select="reduceCounterValue"/>
                                </reduceCounterValue>
                            </counter>

                        </xsl:when>
                        <xsl:when test="name='HDFS_BYTES_WRITTEN'">
                            <counter>
                                <name>
                                    <xsl:value-of select="name"/>
                                </name>
                                <totalCounterValue>
                                    <xsl:value-of select="totalCounterValue"/>
                                </totalCounterValue>
                                <mapCounterValue>
                                    <xsl:value-of select="mapCounterValue"/>
                                </mapCounterValue>
                                <reduceCounterValue>
                                    <xsl:value-of select="reduceCounterValue"/>
                                </reduceCounterValue>
                            </counter>

                        </xsl:when>
                        <!--############################################-->

                        <xsl:when test="name='MB_MILLIS_MAPS'">
                            <counter>
                                <name>
                                    <xsl:value-of select="name"/>
                                </name>
                                <totalCounterValue>
                                    <xsl:value-of select="totalCounterValue"/>
                                </totalCounterValue>
                                <mapCounterValue>
                                    <xsl:value-of select="mapCounterValue"/>
                                </mapCounterValue>
                                <reduceCounterValue>
                                    <xsl:value-of select="reduceCounterValue"/>
                                </reduceCounterValue>
                            </counter>

                        </xsl:when>
                        <xsl:when test="name='MB_MILLIS_REDUCES'">
                            <counter>
                                <name>
                                    <xsl:value-of select="name"/>
                                </name>
                                <totalCounterValue>
                                    <xsl:value-of select="totalCounterValue"/>
                                </totalCounterValue>
                                <mapCounterValue>
                                    <xsl:value-of select="mapCounterValue"/>
                                </mapCounterValue>
                                <reduceCounterValue>
                                    <xsl:value-of select="reduceCounterValue"/>
                                </reduceCounterValue>
                            </counter>

                        </xsl:when>
                        <xsl:when test="name='VCORES_MILLIS_MAPS'">
                            <counter>
                                <name>
                                    <xsl:value-of select="name"/>
                                </name>
                                <totalCounterValue>
                                    <xsl:value-of select="totalCounterValue"/>
                                </totalCounterValue>
                                <mapCounterValue>
                                    <xsl:value-of select="mapCounterValue"/>
                                </mapCounterValue>
                                <reduceCounterValue>
                                    <xsl:value-of select="reduceCounterValue"/>
                                </reduceCounterValue>
                            </counter>

                        </xsl:when>
                        <xsl:when test="name='VCORES_MILLIS_REDUCES'">
                            <counter>
                                <name>
                                    <xsl:value-of select="name"/>
                                </name>
                                <totalCounterValue>
                                    <xsl:value-of select="totalCounterValue"/>
                                </totalCounterValue>
                                <mapCounterValue>
                                    <xsl:value-of select="mapCounterValue"/>
                                </mapCounterValue>
                                <reduceCounterValue>
                                    <xsl:value-of select="reduceCounterValue"/>
                                </reduceCounterValue>
                            </counter>

                        </xsl:when>

                        <xsl:when test="name='CPU_MILLISECONDS'">
                            <counter>
                                <name>
                                    <xsl:value-of select="name"/>
                                </name>
                                <totalCounterValue>
                                    <xsl:value-of select="totalCounterValue"/>
                                </totalCounterValue>
                                <mapCounterValue>
                                    <xsl:value-of select="mapCounterValue"/>
                                </mapCounterValue>
                                <reduceCounterValue>
                                    <xsl:value-of select="reduceCounterValue"/>
                                </reduceCounterValue>
                            </counter>
                        </xsl:when>
                    </xsl:choose>
                </xsl:for-each>

            </counterGroup>
        </jobCounters>
    </xsl:template>

</xsl:stylesheet>