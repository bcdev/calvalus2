package com.bc.calvalus.processing.fire;

import org.junit.Test;

import java.awt.*;

import static org.junit.Assert.assertEquals;

public class FirePixelReducerTest {

    @Test
    public void testCreateBaseFilename() throws Exception {
        assertEquals("20030501-ESACCI-L3S_FIRE-BA-MERIS-AREA_3-v02.0-fv04.0", FirePixelReducer.createBaseFilename("2003", "05", FirePixelProductArea.EUROPE));
        assertEquals("20101001-ESACCI-L3S_FIRE-BA-MERIS-AREA_4-v02.0-fv04.0", FirePixelReducer.createBaseFilename("2010", "10", FirePixelProductArea.ASIA));
    }

    @Test
    public void testComputeFullTargetWidth() throws Exception {
        assertEquals(32400, FirePixelReducer.computeFullTargetWidth(FirePixelProductArea.EUROPE));
        assertEquals(46800, FirePixelReducer.computeFullTargetWidth(FirePixelProductArea.ASIA));
        assertEquals(46800, FirePixelReducer.computeFullTargetWidth(FirePixelProductArea.NORTH_AMERICA));
    }

    @Test
    public void testComputeFullTargetHeight() throws Exception {
        assertEquals(25200, FirePixelReducer.computeFullTargetHeight(FirePixelProductArea.EUROPE));
        assertEquals(32400, FirePixelReducer.computeFullTargetHeight(FirePixelProductArea.ASIA));
    }

    @Test
    public void testComputeTargetRect() throws Exception {
        assertEquals(new Rectangle(0, 2520, 46800, 23040), FirePixelReducer.computeTargetRect(FirePixelProductArea.NORTH_AMERICA));
        assertEquals(new Rectangle(1440, 2520, 28440, 20880), FirePixelReducer.computeTargetRect(FirePixelProductArea.EUROPE));
    }
}
