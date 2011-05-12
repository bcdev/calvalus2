package com.bc.calvalus.processing.ta;

import com.bc.calvalus.binning.TemporalBin;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.Assert.*;

public class TAPointTest {
    private TAPoint point;

    @Before
    public void setUp() throws Exception {
        TemporalBin temporalBin = new TemporalBin(-1, 2);
        temporalBin.getFeatureValues()[0] = 1.2F;
        temporalBin.getFeatureValues()[1] = 3.4F;
        point = new TAPoint("Mediteranean", "2011-01-01", "2011-01-02", temporalBin);
    }

    @Test
    public void testConstruction() throws Exception {
        assertEquals("Mediteranean", point.getRegionName());
        assertEquals("2011-01-01", point.getStartDate());
        assertEquals("2011-01-02", point.getStopDate());
        assertNotNull(point.getTemporalBin());
        assertEquals(1.2F, point.getTemporalBin().getFeatureValues()[0], 1E-5F);
        assertEquals(3.4F, point.getTemporalBin().getFeatureValues()[1], 1E-5F);
    }

    @Test
    public void testSerialisation() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        point.write(new DataOutputStream(out));
        byte[] rawData = out.toByteArray();

        TAPoint point2 = TAPoint.read(new DataInputStream(new ByteArrayInputStream(rawData)));
        assertEquals("Mediteranean", point2.getRegionName());
        assertEquals("2011-01-01", point2.getStartDate());
        assertEquals("2011-01-02", point2.getStopDate());
        assertNotNull(point2.getTemporalBin());
        assertEquals(1.2F, point2.getTemporalBin().getFeatureValues()[0], 1E-5F);
        assertEquals(3.4F, point2.getTemporalBin().getFeatureValues()[1], 1E-5F);
    }

    @Test
    public void testToString() throws Exception {
        assertEquals("TAPoint{" +
                             "regionName=Mediteranean, " +
                             "startDate=2011-01-01, " +
                             "stopDate=2011-01-02, " +
                             "temporalBin=TemporalBin{index=-1, numObs=0, numPasses=0, featureValues=[1.2, 3.4]}" +
                             "}",
                     point.toString());
    }
}
