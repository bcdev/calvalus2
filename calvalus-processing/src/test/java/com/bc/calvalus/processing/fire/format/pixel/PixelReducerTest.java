package com.bc.calvalus.processing.fire.format.pixel;

import org.junit.Assert;
import org.junit.Test;

import java.awt.Rectangle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class PixelReducerTest {

    @Test
    public void testCreateBaseFilename() throws Exception {
        Assert.assertEquals("20030501-ESACCI-L3S_FIRE-BA-MERIS-AREA_3-v02.0-fv04.0-JD", PixelReducer.createBaseFilename("2003", "05", PixelProductArea.EUROPE, PixelVariableType.DAY_OF_YEAR.bandName));
        assertEquals("20101001-ESACCI-L3S_FIRE-BA-MERIS-AREA_4-v02.0-fv04.0-CL", PixelReducer.createBaseFilename("2010", "10", PixelProductArea.ASIA, PixelVariableType.CONFIDENCE_LEVEL.bandName));
    }

    @Test
    public void testComputeFullTargetWidth() throws Exception {
        assertEquals(32400, PixelReducer.computeFullTargetWidth(PixelProductArea.EUROPE));
        assertEquals(46800, PixelReducer.computeFullTargetWidth(PixelProductArea.ASIA));
        assertEquals(46800, PixelReducer.computeFullTargetWidth(PixelProductArea.NORTH_AMERICA));
    }

    @Test
    public void testComputeFullTargetHeight() throws Exception {
        assertEquals(25200, PixelReducer.computeFullTargetHeight(PixelProductArea.EUROPE));
        assertEquals(32400, PixelReducer.computeFullTargetHeight(PixelProductArea.ASIA));
    }

    @Test
    public void testComputeTargetRect() throws Exception {
        assertEquals(new Rectangle(0, 2520, 46800, 23040), PixelReducer.computeTargetRect(PixelProductArea.NORTH_AMERICA));
        assertEquals(new Rectangle(1440, 2520, 28440, 20880), PixelReducer.computeTargetRect(PixelProductArea.EUROPE));
        assertEquals(new Rectangle(1080, 2520, 45720, 29880), PixelReducer.computeTargetRect(PixelProductArea.ASIA));
    }

    @Test
    public void testComputeTargetWidth() throws Exception {
        assertEquals(45720, PixelReducer.computeTargetWidth(PixelProductArea.ASIA));
    }

    @Test
    public void testComputeTargetHeightWidth() throws Exception {
        assertEquals(29880, PixelReducer.computeTargetHeight(PixelProductArea.ASIA));
    }

    @Test
    public void testGetLeftTargetXForTile() throws Exception {
        assertEquals(6120, PixelReducer.getLeftTargetXForTile(PixelProductArea.ASIA, "2006-08-v04h25"));
        assertEquals(0, PixelReducer.getLeftTargetXForTile(PixelProductArea.ASIA, "2006-08-v04h23"));
        assertEquals(2520, PixelReducer.getLeftTargetXForTile(PixelProductArea.ASIA, "2006-08-v04h24"));

        assertEquals(9360, PixelReducer.getLeftTargetXForTile(PixelProductArea.EUROPE, "2006-08-v04h18"));
        assertEquals(0, PixelReducer.getLeftTargetXForTile(PixelProductArea.EUROPE, "2006-08-v04h15"));
        assertEquals(2160, PixelReducer.getLeftTargetXForTile(PixelProductArea.EUROPE, "2006-08-v04h16"));

        assertEquals(0, PixelReducer.getLeftTargetXForTile(PixelProductArea.NORTH_AMERICA, "2006-08-v04h00"));
    }

    @Test
    public void testGetTopTargetYForTile() throws Exception {
        assertEquals(11880, PixelReducer.getTopTargetYForTile(PixelProductArea.ASIA, "2006-08-v04h25"));
        assertEquals(0, PixelReducer.getTopTargetYForTile(PixelProductArea.ASIA, "2006-08-v00h25"));
        assertEquals(1080, PixelReducer.getTopTargetYForTile(PixelProductArea.ASIA, "2006-08-v01h25"));

        assertEquals(0, PixelReducer.getTopTargetYForTile(PixelProductArea.AUSTRALIA, "2006-08-v09h34"));
    }

    @Test
    public void testGetLeftSourceX() throws Exception {
        assertEquals(1080, PixelReducer.getLeftSourceXForTile(PixelProductArea.ASIA, "2008-06-v02h23"));
        assertEquals(0, PixelReducer.getLeftSourceXForTile(PixelProductArea.ASIA, "2008-06-v02h24"));
        assertEquals(0, PixelReducer.getLeftSourceXForTile(PixelProductArea.ASIA, "2008-06-v02h25"));
    }

    @Test
    public void testGetTargetValues() throws Exception {
        int[] values = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        int[] data = PixelReducer.getTargetValues(0, 3, 0, 3, values);
        assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, data);

        data = PixelReducer.getTargetValues(1, 3, 0, 3, values);
        assertArrayEquals(new int[]{1, 2, 3, 5, 6, 7, 9, 10, 11, 13, 14, 15}, data);

        data = PixelReducer.getTargetValues(0, 3, 2, 3, values);
        assertArrayEquals(new int[]{8, 9, 10, 11, 12, 13, 14, 15}, data);

        data = PixelReducer.getTargetValues(0, 2, 0, 3, values);
        assertArrayEquals(new int[]{0, 1, 2, 4, 5, 6, 8, 9, 10, 12, 13, 14}, data);

        data = PixelReducer.getTargetValues(1, 3, 1, 3, values);
        assertArrayEquals(new int[]{5, 6, 7, 9, 10, 11, 13, 14, 15}, data);
    }
}
