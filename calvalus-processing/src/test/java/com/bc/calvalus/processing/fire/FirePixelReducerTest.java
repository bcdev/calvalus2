package com.bc.calvalus.processing.fire;

import org.junit.Test;

import java.awt.Rectangle;
import java.util.Arrays;

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
    public void testGetMaxX() throws Exception {
        assertEquals(9719, FirePixelReducer.getMaxX(FirePixelProductArea.ASIA, "2008-08-v04h25"));
        assertEquals(1079, FirePixelReducer.getMaxX(FirePixelProductArea.ASIA, "2008-08-v04h23"));

        assertEquals(27359, FirePixelReducer.getMaxX(FirePixelProductArea.EUROPE, "2006-08-v04h22"));
        assertEquals(28439, FirePixelReducer.getMaxX(FirePixelProductArea.EUROPE, "2006-08-v04h23"));

        assertEquals(30599, FirePixelReducer.getMaxX(FirePixelProductArea.AUSTRALIA, "2008-08-v09h35"));
    }

    @Test
    public void name() throws Exception {
        int[] values = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        int[] data = getInts(values, 3, 0, 3, 0);
        assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, data);

        data = getInts(values, 3, 1, 3, 0);
        assertArrayEquals(new int[]{1, 2, 3, 5, 6, 7, 9, 10, 11, 13, 14, 15}, data);

        data = getInts(values, 3, 0, 3, 2);
        assertArrayEquals(new int[]{8, 9, 10, 11, 12, 13, 14, 15}, data);

        data = getInts(values, 2, 0, 3, 0);
        System.out.println(Arrays.toString(data));
        assertArrayEquals(new int[]{0, 1, 2, 4, 5, 6, 8, 9, 10, 12, 13, 14}, data);
    }

    private int[] getInts(int[] values, int maxX, int leftTargetX, int maxY, int topTargetY) {
        int width;
        int height;
        int[] data;
        width = maxX - leftTargetX + 1;
        height = maxY - topTargetY + 1;
        data = new int[(width) * (height)];

        for (int y = topTargetY; y <= maxY; y++) {
            // continue here; needed: full width of value array in order to determine srcPos
            int srcPos = y * width + (y + 1) * leftTargetX;
            System.arraycopy(values, srcPos, data, (y - topTargetY) * width, width);
        }
        return data;
    }
}
