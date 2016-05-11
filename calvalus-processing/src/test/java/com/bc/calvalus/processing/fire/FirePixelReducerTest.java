package com.bc.calvalus.processing.fire;

import org.junit.Test;

import java.awt.Rectangle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class FirePixelReducerTest {

    @Test
    public void testCreateBaseFilename() throws Exception {
        assertEquals("20030501-ESACCI-L3S_FIRE-BA-MERIS-AREA_3-v02.0-fv04.0-JD", FirePixelReducer.createBaseFilename("2003", "05", FirePixelProductArea.EUROPE, FirePixelVariableType.DAY_OF_YEAR.bandName));
        assertEquals("20101001-ESACCI-L3S_FIRE-BA-MERIS-AREA_4-v02.0-fv04.0-CL", FirePixelReducer.createBaseFilename("2010", "10", FirePixelProductArea.ASIA, FirePixelVariableType.CONFIDENCE_LEVEL.bandName));
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
        assertEquals(new Rectangle(1080, 2520, 45720, 29880), FirePixelReducer.computeTargetRect(FirePixelProductArea.ASIA));
    }

    @Test
    public void testComputeTargetWidth() throws Exception {
        assertEquals(45720, FirePixelReducer.computeTargetWidth(FirePixelProductArea.ASIA));
    }

    @Test
    public void testComputeTargetHeightWidth() throws Exception {
        assertEquals(29880, FirePixelReducer.computeTargetHeight(FirePixelProductArea.ASIA));
    }

    @Test
    public void testGetLeftTargetXForTile() throws Exception {
        assertEquals(6120, FirePixelReducer.getLeftTargetXForTile(FirePixelProductArea.ASIA, "2006-08-v04h25"));
        assertEquals(0, FirePixelReducer.getLeftTargetXForTile(FirePixelProductArea.ASIA, "2006-08-v04h23"));
        assertEquals(2520, FirePixelReducer.getLeftTargetXForTile(FirePixelProductArea.ASIA, "2006-08-v04h24"));

        assertEquals(9360, FirePixelReducer.getLeftTargetXForTile(FirePixelProductArea.EUROPE, "2006-08-v04h18"));
        assertEquals(0, FirePixelReducer.getLeftTargetXForTile(FirePixelProductArea.EUROPE, "2006-08-v04h15"));
        assertEquals(2160, FirePixelReducer.getLeftTargetXForTile(FirePixelProductArea.EUROPE, "2006-08-v04h16"));

        assertEquals(0, FirePixelReducer.getLeftTargetXForTile(FirePixelProductArea.NORTH_AMERICA, "2006-08-v04h00"));
    }

    @Test
    public void testGetTopTargetYForTile() throws Exception {
        assertEquals(11880, FirePixelReducer.getTopTargetYForTile(FirePixelProductArea.ASIA, "2006-08-v04h25"));
        assertEquals(0, FirePixelReducer.getTopTargetYForTile(FirePixelProductArea.ASIA, "2006-08-v00h25"));
        assertEquals(1080, FirePixelReducer.getTopTargetYForTile(FirePixelProductArea.ASIA, "2006-08-v01h25"));

        assertEquals(0, FirePixelReducer.getTopTargetYForTile(FirePixelProductArea.AUSTRALIA, "2006-08-v09h34"));
    }

    @Test
    public void testGetLeftSourceX() throws Exception {
        assertEquals(1080, FirePixelReducer.getLeftSourceXForTile(FirePixelProductArea.ASIA, "2008-06-v02h23"));
        assertEquals(0, FirePixelReducer.getLeftSourceXForTile(FirePixelProductArea.ASIA, "2008-06-v02h24"));
        assertEquals(0, FirePixelReducer.getLeftSourceXForTile(FirePixelProductArea.ASIA, "2008-06-v02h25"));
    }

    @Test
    public void testGetTargetValues() throws Exception {
        int[] values = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        int[] data = FirePixelReducer.getTargetValues(0, 3, 0, 3, values);
        assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, data);

        data = FirePixelReducer.getTargetValues(1, 3, 0, 3, values);
        assertArrayEquals(new int[]{1, 2, 3, 5, 6, 7, 9, 10, 11, 13, 14, 15}, data);

        data = FirePixelReducer.getTargetValues(0, 3, 2, 3, values);
        assertArrayEquals(new int[]{8, 9, 10, 11, 12, 13, 14, 15}, data);

        data = FirePixelReducer.getTargetValues(0, 2, 0, 3, values);
        assertArrayEquals(new int[]{0, 1, 2, 4, 5, 6, 8, 9, 10, 12, 13, 14}, data);

        data = FirePixelReducer.getTargetValues(1, 3, 1, 3, values);
        assertArrayEquals(new int[]{5, 6, 7, 9, 10, 11, 13, 14, 15}, data);
    }
}
