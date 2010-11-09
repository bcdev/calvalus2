package com.bc.calvalus.binning;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IsinGridTest {

    @Test
    public void testGrid() {
        IsinBinningGrid grid = new IsinBinningGrid();

        assertEquals(2160, grid.getNumRows());
        assertEquals(5940422, grid.getNumBins());

        assertEquals(3, grid.getNumCols(0));
        assertEquals(4320, grid.getNumCols(2160 / 2));
        assertEquals(4320, grid.getNumCols(2160 / 2 + 1));
        assertEquals(3, grid.getNumCols(2160 - 1));

        assertEquals(0, grid.getBinIndex(-90.0, -180.0));
        assertEquals(2972371, grid.getBinIndex(0.0, 0.0));
        assertEquals(5940421, grid.getBinIndex(+90.0, +180.0));

    }
}