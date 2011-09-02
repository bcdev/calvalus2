<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:template match="/">

<!--
    Note: In order to further develop test this XSD,
          run com.bc.calvalus.processing.ma.ReportGeneratorTest.

    TODO:
    * Provide processing statistics, e.g. total time, #tasks, #bytes, #input/output records, hadoop counters
    * Make the NTML nicer, use CSS
-->

<html>
    <head>
        <title>Match-up Analysis</title>
        <link rel="stylesheet" type="text/css" href="styleset.css"/>
    </head>
<body>

<h2>Match-up Analysis</h2>

<h3>Processing Information</h3>

<table>
    <tr>
        <td class="name">Performed at:</td>
        <td class="value"><xsl:value-of select="analysisSummary/performedAt"/></td>
    </tr>
    <tr>
        <td class="name">Number of match-ups:</td>
        <td class="value"><xsl:value-of select="analysisSummary/recordCount"/></td>
    </tr>
</table>

<h3>Analysis Parameters</h3>

<table>
    <xsl:for-each select="analysisSummary/configuration/property">
    <xsl:choose>
    <xsl:when test="name='calvalus.calvalus.bundle'">
        <tr>
            <td class="name">Calvalus bundle:</td>
            <td class="value"><xsl:value-of select="value"/></td>
        </tr>
    </xsl:when>
    <xsl:when test="name='calvalus.beam.bundle'">
        <tr>
            <td class="name">BEAM bundle:</td>
            <td class="value"><xsl:value-of select="value"/></td>
        </tr>
    </xsl:when>
    <xsl:when test="name='calvalus.l2.bundle'">
        <tr>
            <td class="name">Processor bundle:</td>
            <td class="value"><xsl:value-of select="value"/></td>
        </tr>
    </xsl:when>
    <xsl:when test="name='calvalus.l2.operator'">
        <tr>
            <td class="name">Processor name:</td>
            <td class="value"><xsl:value-of select="value"/></td>
        </tr>
    </xsl:when>
    <xsl:when test="name='calvalus.l2.parameters'">
        <tr>
            <td class="name">Processor parameters:</td>
            <td class="value"><xsl:value-of select="value"/></td>
        </tr>
    </xsl:when>
    <xsl:when test="name='calvalus.ma.parameters'">
        <tr>
            <td class="name">Input reference dataset:</td>
            <td class="value"><xsl:value-of select="value/parameters/recordSourceUrl"/></td>
        </tr>
        <tr>
            <td class="name">Output group name:</td>
            <td class="value"><xsl:value-of select="value/parameters/outputGroupName"/></td>
        </tr>
        <tr>
            <td class="name">Output date/time format:</td>
            <td class="value"><xsl:value-of select="value/parameters/outputTimeFormat"/></td>
        </tr>
        <tr>
            <td class="name">Macro pixel size:</td>
            <td class="value"><xsl:value-of select="value/parameters/macroPixelSize"/></td>
        </tr>
        <tr>
            <td class="name">Max. time difference (h):</td>
            <td class="value"><xsl:value-of select="value/parameters/maxTimeDifference"/></td>
        </tr>
        <tr>
            <td class="name">Good-pixel expression:</td>
            <td class="value"><xsl:value-of select="value/parameters/goodPixelExpression"/></td>
        </tr>
        <tr>
            <td class="name">Good-record expression:</td>
            <td class="value"><xsl:value-of select="value/parameters/goodRecordExpression"/></td>
        </tr>
        <tr>
            <td class="name">Copy input:</td>
            <td class="value"><xsl:value-of select="value/parameters/copyInput"/></td>
        </tr>
        <tr>
            <td class="name">Filtered mean coefficient:</td>
            <td class="value"><xsl:value-of select="value/parameters/filteredMeanCoeff"/></td>
        </tr>
    </xsl:when>
    </xsl:choose>
    </xsl:for-each>
</table>

<h3>Scatter plots</h3>

<table>
    <xsl:for-each select="analysisSummary/dataset">
    <tr>
        <td class="dataset-info">
            <table>
                <tr>
                    <td class="name">Satellite variable:</td>
                    <td class="value"><xsl:value-of select="satelliteVariable"/></td>
                </tr>
                <tr>
                    <td class="name">Reference variable:</td>
                    <td class="value"><xsl:value-of select="referenceVariable"/></td>
                </tr>
                <tr>
                    <td class="name">Number of data points:</td>
                    <td class="value"><xsl:value-of select="statistics/numDataPoints"/></td>
                </tr>
                <tr>
                    <td class="name">Lin. regression interception:</td>
                    <td class="value"><xsl:value-of select="statistics/regressionInter"/></td>
                </tr>
                <tr>
                    <td class="name">Lin. regression slope:</td>
                    <td class="value"><xsl:value-of select="statistics/regressionSlope"/></td>
                </tr>
            </table>
        </td>
        <td class="dataset-graph">
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
