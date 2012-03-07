package org.esa.beam.binning.support;

import org.esa.beam.binning.operator.BinWriter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Norman Fomferra
 */
public class BinWriterTest {

    @Test
    public void testConvertCalvalusToSeadasBinIndex() throws Exception {
        // 3 + 8 + 12 + 12 + 8 + 3 bins
        final IsinBinningGrid grid = new IsinBinningGrid(6);

        assertEquals(0, grid.getFirstBinIndex(0));
        assertEquals(3, grid.getFirstBinIndex(1));
        assertEquals(11, grid.getFirstBinIndex(2));
        assertEquals(23, grid.getFirstBinIndex(3));
        assertEquals(35, grid.getFirstBinIndex(4));
        assertEquals(43, grid.getFirstBinIndex(5));
        assertEquals(46, grid.getNumBins());

        //     Calvalus     SeaDAS
        // 0    0 ..  2     43 .. 45
        // 1    3 .. 10     35 .. 42
        // 2   11 .. 22     23 .. 34
        // 3   23 .. 34     11 .. 22
        // 4   35 .. 42      3 .. 10
        // 5   43 .. 45      0 ..  2

        assertEquals(43, BinWriter.convertCalvalusToSeadasBinIndex(grid, 0));
        assertEquals(45, BinWriter.convertCalvalusToSeadasBinIndex(grid, 2));
        assertEquals(35, BinWriter.convertCalvalusToSeadasBinIndex(grid, 3));
        assertEquals(42, BinWriter.convertCalvalusToSeadasBinIndex(grid, 10));
        assertEquals(23, BinWriter.convertCalvalusToSeadasBinIndex(grid, 11));
        assertEquals(34, BinWriter.convertCalvalusToSeadasBinIndex(grid, 22));
        assertEquals(11, BinWriter.convertCalvalusToSeadasBinIndex(grid, 23));
        assertEquals(22, BinWriter.convertCalvalusToSeadasBinIndex(grid, 34));
        assertEquals(3, BinWriter.convertCalvalusToSeadasBinIndex(grid, 35));
        assertEquals(10, BinWriter.convertCalvalusToSeadasBinIndex(grid, 42));
        assertEquals(0, BinWriter.convertCalvalusToSeadasBinIndex(grid, 43));
        assertEquals(2, BinWriter.convertCalvalusToSeadasBinIndex(grid, 45));
    }

}
