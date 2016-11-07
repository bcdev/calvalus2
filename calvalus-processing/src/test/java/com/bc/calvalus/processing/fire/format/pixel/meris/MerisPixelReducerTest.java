package com.bc.calvalus.processing.fire.format.pixel.meris;

import com.bc.calvalus.processing.fire.format.MerisStrategy;
import org.junit.Test;

import java.awt.Rectangle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MerisPixelReducerTest {

    @Test
    public void testCreateBaseFilename() throws Exception {
        assertEquals("20030501-ESACCI-L3S_FIRE-BA-MERIS-AREA_3-fv04.1", MerisPixelMergeMapper.createBaseFilename("2003", "05", "v04.1", new MerisStrategy().getArea("EUROPE")));
        assertEquals("20101001-ESACCI-L3S_FIRE-BA-MERIS-AREA_4-fv04.1", MerisPixelMergeMapper.createBaseFilename("2010", "10", "v04.1", new MerisStrategy().getArea("ASIA")));
    }

    @Test
    public void testComputeFullTargetWidth() throws Exception {
        assertEquals(32400, MerisPixelReducer.computeFullTargetWidth(new MerisStrategy().getArea("EUROPE")));
        assertEquals(46800, MerisPixelReducer.computeFullTargetWidth(new MerisStrategy().getArea("ASIA")));
        assertEquals(46800, MerisPixelReducer.computeFullTargetWidth(new MerisStrategy().getArea("NORTH_AMERICA")));
    }

    @Test
    public void testComputeFullTargetHeight() throws Exception {
        assertEquals(25200, MerisPixelReducer.computeFullTargetHeight(new MerisStrategy().getArea("EUROPE")));
        assertEquals(32400, MerisPixelReducer.computeFullTargetHeight(new MerisStrategy().getArea("ASIA")));
    }

    @Test
    public void testComputeTargetRect() throws Exception {
        assertEquals(new Rectangle(0, 2520, 46800, 23040), MerisPixelReducer.computeTargetRect(new MerisStrategy().getArea("NORTH_AMERICA")));
        assertEquals(new Rectangle(1440, 2520, 28440, 20880), MerisPixelReducer.computeTargetRect(new MerisStrategy().getArea("EUROPE")));
        assertEquals(new Rectangle(1080, 2520, 45720, 29880), MerisPixelReducer.computeTargetRect(new MerisStrategy().getArea("ASIA")));
    }

    @Test
    public void testComputeTargetWidth() throws Exception {
        assertEquals(45720, MerisPixelReducer.computeTargetWidth(new MerisStrategy().getArea("ASIA")));
    }

    @Test
    public void testComputeTargetHeightWidth() throws Exception {
        assertEquals(29880, MerisPixelReducer.computeTargetHeight(new MerisStrategy().getArea("ASIA")));
    }

    @Test
    public void testGetLeftTargetXForTile() throws Exception {
        assertEquals(6120, MerisPixelReducer.getLeftTargetXForTile(new MerisStrategy().getArea("ASIA"), "2006-08-v04h25"));
        assertEquals(0, MerisPixelReducer.getLeftTargetXForTile(new MerisStrategy().getArea("ASIA"), "2006-08-v04h23"));
        assertEquals(2520, MerisPixelReducer.getLeftTargetXForTile(new MerisStrategy().getArea("ASIA"), "2006-08-v04h24"));

        assertEquals(9360, MerisPixelReducer.getLeftTargetXForTile(new MerisStrategy().getArea("EUROPE"), "2006-08-v04h18"));
        assertEquals(0, MerisPixelReducer.getLeftTargetXForTile(new MerisStrategy().getArea("EUROPE"), "2006-08-v04h15"));
        assertEquals(2160, MerisPixelReducer.getLeftTargetXForTile(new MerisStrategy().getArea("EUROPE"), "2006-08-v04h16"));

        assertEquals(0, MerisPixelReducer.getLeftTargetXForTile(new MerisStrategy().getArea("NORTH_AMERICA"), "2006-08-v04h00"));
    }

    @Test
    public void testGetTopTargetYForTile() throws Exception {
        assertEquals(11880, MerisPixelReducer.getTopTargetYForTile(new MerisStrategy().getArea("ASIA"), "2006-08-v04h25"));
        assertEquals(0, MerisPixelReducer.getTopTargetYForTile(new MerisStrategy().getArea("ASIA"), "2006-08-v00h25"));
        assertEquals(1080, MerisPixelReducer.getTopTargetYForTile(new MerisStrategy().getArea("ASIA"), "2006-08-v01h25"));

        assertEquals(0, MerisPixelReducer.getTopTargetYForTile(new MerisStrategy().getArea("AUSTRALIA"), "2006-08-v09h34"));
    }

    @Test
    public void testGetLeftSourceX() throws Exception {
        assertEquals(1080, MerisPixelReducer.getLeftSourceXForTile(new MerisStrategy().getArea("ASIA"), "2008-06-v02h23"));
        assertEquals(0, MerisPixelReducer.getLeftSourceXForTile(new MerisStrategy().getArea("ASIA"), "2008-06-v02h24"));
        assertEquals(0, MerisPixelReducer.getLeftSourceXForTile(new MerisStrategy().getArea("ASIA"), "2008-06-v02h25"));
    }

    @Test
    public void testGetTargetValues() throws Exception {
        short[] values = new short[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        short[] data = MerisPixelReducer.getTargetValues(0, 3, 0, 3, values);
        assertArrayEquals(new short[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, data);

        data = MerisPixelReducer.getTargetValues(1, 3, 0, 3, values);
        assertArrayEquals(new short[]{1, 2, 3, 5, 6, 7, 9, 10, 11, 13, 14, 15}, data);

        data = MerisPixelReducer.getTargetValues(0, 3, 2, 3, values);
        assertArrayEquals(new short[]{8, 9, 10, 11, 12, 13, 14, 15}, data);

        data = MerisPixelReducer.getTargetValues(0, 2, 0, 3, values);
        assertArrayEquals(new short[]{0, 1, 2, 4, 5, 6, 8, 9, 10, 12, 13, 14}, data);

        data = MerisPixelReducer.getTargetValues(1, 3, 1, 3, values);
        assertArrayEquals(new short[]{5, 6, 7, 9, 10, 11, 13, 14, 15}, data);
    }
}
