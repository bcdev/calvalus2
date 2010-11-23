package com.bc.calvalus.b3;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class IsinBinningGridTest {
    @Test
    public void testConstructors() {
        IsinBinningGrid grid = new IsinBinningGrid();
        assertEquals(2160, grid.getNumRows());

        grid = new IsinBinningGrid(2);
        assertEquals(2, grid.getNumRows());

        try {
            new IsinBinningGrid(1);
            fail("IAE expected");
        } catch (IllegalArgumentException e) {
        }

        try {
            new IsinBinningGrid(0);
            fail("IAE expected");
        } catch (IllegalArgumentException e) {
        }

        try {
            new IsinBinningGrid(-1);
            fail("IAE expected");
        } catch (IllegalArgumentException e) {
        }

        try {
            new IsinBinningGrid(9);
            fail("IAE expected");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testGrid() {
        IsinBinningGrid grid = new IsinBinningGrid();

        assertEquals(2160, grid.getNumRows());
        assertEquals(5940422, grid.getNumBins());

        assertEquals(3, grid.getNumCols(0));
        assertEquals(2 * 2160, grid.getNumCols(2160 / 2));
        assertEquals(2 * 2160, grid.getNumCols(2160 / 2 + 1));
        assertEquals(3, grid.getNumCols(2160 - 1));

        assertEquals(0, grid.getBinIndex(-90.0, -180.0));
        assertEquals(2972371, grid.getBinIndex(0.0, 0.0));
        assertEquals(5940421, grid.getBinIndex(+90.0, +180.0));

    }

    @Test
    public void testPerformance() {

        long t0 = System.nanoTime();
        int N = 1000;
        for (int i = 0; i < N; i++) {
            new IsinBinningGrid();
        }
        long t1 = System.nanoTime();

        final double seconds = (t1 - t0) / 1.0E9;
        assertTrue("Bad performance in IsinBinningGrid detected, took " + seconds + " seconds",
                   seconds < 1.0);
    }


    @Test
    public void testGetRowIndex() {

        // 3, 8, 12, 12, 8, 3
        IsinBinningGrid grid = new IsinBinningGrid(6);
        try {
            grid.getRowIndex(-1);
            fail("ArrayIndexOutOfBoundsException?");
        } catch (ArrayIndexOutOfBoundsException e) {
        }
        assertEquals(0, grid.getRowIndex(0));
        assertEquals(0, grid.getRowIndex(1));
        assertEquals(0, grid.getRowIndex(2));
        assertEquals(1, grid.getRowIndex(3));
        assertEquals(1, grid.getRowIndex(4));
        assertEquals(1, grid.getRowIndex(5));
        assertEquals(1, grid.getRowIndex(6));
        assertEquals(1, grid.getRowIndex(7));
        assertEquals(1, grid.getRowIndex(8));
        assertEquals(1, grid.getRowIndex(9));
        assertEquals(1, grid.getRowIndex(10));
        assertEquals(2, grid.getRowIndex(11));
        assertEquals(2, grid.getRowIndex(12));
        assertEquals(2, grid.getRowIndex(13));
        assertEquals(2, grid.getRowIndex(14));
        assertEquals(2, grid.getRowIndex(15));
        assertEquals(2, grid.getRowIndex(16));
        assertEquals(2, grid.getRowIndex(17));
        assertEquals(2, grid.getRowIndex(18));
        assertEquals(2, grid.getRowIndex(19));
        assertEquals(2, grid.getRowIndex(20));
        assertEquals(2, grid.getRowIndex(21));
        assertEquals(2, grid.getRowIndex(22));
        assertEquals(3, grid.getRowIndex(23));
        assertEquals(3, grid.getRowIndex(24));
        assertEquals(3, grid.getRowIndex(25));
        assertEquals(3, grid.getRowIndex(26));
        assertEquals(3, grid.getRowIndex(27));
        assertEquals(3, grid.getRowIndex(28));
        assertEquals(3, grid.getRowIndex(29));
        assertEquals(3, grid.getRowIndex(30));
        assertEquals(3, grid.getRowIndex(31));
        assertEquals(3, grid.getRowIndex(32));
        assertEquals(3, grid.getRowIndex(33));
        assertEquals(3, grid.getRowIndex(34));
        assertEquals(4, grid.getRowIndex(35));
        assertEquals(4, grid.getRowIndex(36));
        assertEquals(4, grid.getRowIndex(37));
        assertEquals(4, grid.getRowIndex(38));
        assertEquals(4, grid.getRowIndex(39));
        assertEquals(4, grid.getRowIndex(40));
        assertEquals(4, grid.getRowIndex(41));
        assertEquals(4, grid.getRowIndex(42));
        assertEquals(5, grid.getRowIndex(43));
        assertEquals(5, grid.getRowIndex(44));
        assertEquals(5, grid.getRowIndex(45));
        try {
            grid.getRowIndex(46);
//            fail("ArrayIndexOutOfBoundsException?");
        } catch (ArrayIndexOutOfBoundsException e) {
        }
    }
}