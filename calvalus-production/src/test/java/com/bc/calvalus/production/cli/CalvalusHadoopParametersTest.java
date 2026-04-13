package com.bc.calvalus.production.cli;

import junit.framework.TestCase;
import org.junit.Ignore;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class CalvalusHadoopParametersTest extends TestCase {

    @Ignore
    public void testRaParameters2Region() {
        final String raParameters = "<parameters><regionSource>/windows/tmp/esthub/MT_MKE_2024_buffer_4m_gv.zip</regionSource><regionSourceAttributeName>id</regionSourceAttributeName><goodPixelExpression>true</goodPixelExpression><bands><band><name>Sigma0_VV</name></band><band><name>Sigma0_VH</name></band><band><name>VH_VV</name></band></bands><percentiles>50</percentiles><writePixelValues>false</writePixelValues><writePerRegion>\"false\"</writePerRegion><withRegionEnvelope>true</withRegionEnvelope><withProductNames>true</withProductNames><writeSeparateHistogram>false</writeSeparateHistogram></parameters>";
        final String bbox = new CalvalusHadoopParameters().raParameters2Region(raParameters);
        assertEquals("POLYGON", bbox.substring(0,7));
    }
    @Ignore
    public void testRaParameters2Bbox() {
        final String raParameters = "<parameters><regionSource>/windows/tmp/esthub/MT_MKE_2024_buffer_4m_gv.zip</regionSource><regionSourceAttributeName>id</regionSourceAttributeName><goodPixelExpression>true</goodPixelExpression><bands><band><name>Sigma0_VV</name></band><band><name>Sigma0_VH</name></band><band><name>VH_VV</name></band></bands><percentiles>50</percentiles><writePixelValues>false</writePixelValues><writePerRegion>\"false\"</writePerRegion><withRegionEnvelope>false</withRegionEnvelope><withProductNames>true</withProductNames><writeSeparateHistogram>false</writeSeparateHistogram></parameters>";
        final String bbox = new CalvalusHadoopParameters().raParameters2Bbox(raParameters);
        assertEquals("POLYGON ((21.859249169489523 57.51481100092911, 21.859249169489523 59.66411938914398, 28.133752196663927 59.66411938914398, 28.133752196663927 57.51481100092911, 21.859249169489523 57.51481100092911))", bbox);
    }
}