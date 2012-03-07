package org.esa.beam.binning.support;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Norman Fomferra
 */
public class BinWriterTest {

    @Test
    public void testCalvalusToSeaDasBinIndex() throws Exception {
        // 3 8 12 12 8 3
        final IsinBinningGrid grid = new IsinBinningGrid(6);

        assertEquals(3 + 8 + 12 + 12 + 8 + 3, grid.getNumBins());
        // assertEquals(grid.getNumBins() - 1, convertCalvalusToSeaDasBinIndex(0));
        // assertEquals(0, convertCalvalusToSeaDasBinIndex(grid.getNumBins() - 1));
    }
}
