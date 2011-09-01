<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:template match="/">

<html>
<body>

<h2>Match-up Analysis</h2>

<h3>Processing Information</h3>

<!--
    TODO:
    * Provide processing statistics, e.g. total time, #tasks, #bytes, #input/output records, hadoop counters
    * Make the NTML nicer, use CSS
-->
<table border="0">
    <tr>
        <td>Performed at:</td>
        <td><xsl:value-of select="analysisSummary/performedAt"/></td>
    </tr>
    <tr>
        <td>Number of match-ups:</td>
        <td><xsl:value-of select="analysisSummary/recordCount"/></td>
    </tr>
</table>

<h3>Analysis Parameters</h3>

<table border="0">
    <xsl:for-each select="analysisSummary/configuration/property">
    <xsl:choose>
    <xsl:when test="name='calvalus.calvalus.bundle'">
        <tr>
            <td>Calvalus bundle:</td>
            <td><xsl:value-of select="value"/></td>
        </tr>
    </xsl:when>
    <xsl:when test="name='calvalus.beam.bundle'">
        <tr>
            <td>BEAM bundle:</td>
            <td><xsl:value-of select="value"/></td>
        </tr>
    </xsl:when>
    <xsl:when test="name='calvalus.l2.bundle'">
        <tr>
            <td>Processor bundle:</td>
            <td><xsl:value-of select="value"/></td>
        </tr>
    </xsl:when>
    <xsl:when test="name='calvalus.l2.operator'">
        <tr>
            <td>Processor name:</td>
            <td><xsl:value-of select="value"/></td>
        </tr>
    </xsl:when>
    <xsl:when test="name='calvalus.l2.parameters'">
        <tr>
            <td>Processor parameters:</td>
            <td><xsl:value-of select="value"/></td>
        </tr>
    </xsl:when>
    <xsl:when test="name='calvalus.ma.parameters'">
        <tr>
            <td>Input reference dataset:</td>
            <td><xsl:value-of select="value/parameters/recordSourceUrl"/></td>
        </tr>
        <tr>
            <td>Output group name:</td>
            <td><xsl:value-of select="value/parameters/outputGroupName"/></td>
        </tr>
        <tr>
            <td>Output date/time format:</td>
            <td><xsl:value-of select="value/parameters/outputTimeFormat"/></td>
        </tr>
        <tr>
            <td>Macro pixel size:</td>
            <td><xsl:value-of select="value/parameters/macroPixelSize"/></td>
        </tr>
        <tr>
            <td>Max. time difference (h):</td>
            <td><xsl:value-of select="value/parameters/maxTimeDifference"/></td>
        </tr>
        <tr>
            <td>Good-pixel expression:</td>
            <td><xsl:value-of select="value/parameters/goodPixelExpression"/></td>
        </tr>
        <tr>
            <td>Good-record expression:</td>
            <td><xsl:value-of select="value/parameters/goodRecordExpression"/></td>
        </tr>
        <tr>
            <td>Copy input:</td>
            <td><xsl:value-of select="value/parameters/copyInput"/></td>
        </tr>
        <tr>
            <td>Filtered mean coefficient:</td>
            <td><xsl:value-of select="value/parameters/filteredMeanCoeff"/></td>
        </tr>
    </xsl:when>
    </xsl:choose>
    </xsl:for-each>
</table>

<h3>Scatter plots</h3>

<table border="0">
    <xsl:for-each select="analysisSummary/dataset">
    <tr>
        <td>
            <table border="0">
                <tr>
                    <td>Reference variable:</td>
                    <td><xsl:value-of select="referenceVariable"/></td>
                </tr>
                <tr>
                    <td>Satellite variable:</td>
                    <td><xsl:value-of select="satelliteVariable"/></td>
                </tr>
                <tr>
                    <td>Number of data points:</td>
                    <td><xsl:value-of select="statistics/numDataPoints"/></td>
                </tr>
                <tr>
                    <td>Lin. regression interception:</td>
                    <td><xsl:value-of select="statistics/regressionInter"/></td>
                </tr>
                <tr>
                    <td>Lin. regression slope:</td>
                    <td><xsl:value-of select="statistics/regressionSlope"/></td>
                </tr>
            </table>
        </td>
        <td>
            <img>
            <xsl:attribute name="src">
                <xsl:value-of select="statistics/scatterPlotImage" />
            </xsl:attribute>
            </img>
        </td>
    </tr>
    </xsl:for-each>
</table>

</body>
</html>
</xsl:template>
</xsl:stylesheet>
