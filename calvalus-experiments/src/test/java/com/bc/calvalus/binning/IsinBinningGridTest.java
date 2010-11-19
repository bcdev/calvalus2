package com.bc.calvalus.binning;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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

        System.out.printf("time: %s s%n", ((t1 - t0) / 1E9));
    }
}