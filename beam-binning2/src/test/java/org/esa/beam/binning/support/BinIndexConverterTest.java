package org.esa.beam.binning.support;

import org.esa.beam.binning.operator.SeadasBinningGrid;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Norman Fomferra
 */
public class BinIndexConverterTest {

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

        SeadasBinningGrid converter = new SeadasBinningGrid(grid);

        assertEquals(44, converter.getSeadasBinIndex(0));
        assertEquals(46, converter.getSeadasBinIndex(2));
        assertEquals(36, converter.getSeadasBinIndex(3));
        assertEquals(43, converter.getSeadasBinIndex(10));
        assertEquals(24, converter.getSeadasBinIndex(11));
        assertEquals(35, converter.getSeadasBinIndex(22));
        assertEquals(12, converter.getSeadasBinIndex(23));
        assertEquals(23, converter.getSeadasBinIndex(34));
        assertEquals(4, converter.getSeadasBinIndex(35));
        assertEquals(11, converter.getSeadasBinIndex(42));
        assertEquals(1, converter.getSeadasBinIndex(43));
        assertEquals(3, converter.getSeadasBinIndex(45));
    }

}
