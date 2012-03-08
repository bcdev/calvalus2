package org.esa.beam.binning.operator;

import org.esa.beam.binning.BinningGrid;

/**
 * Utility class that is used to convert from BEAM / Calvalus SEA grid indexes to the ones used in SeaDAS.
 *
 * @author Norman Fomferra
 */
public class SeadasBinningGrid {

    public static final int MAX_NUM_BINS = Integer.MAX_VALUE - 1;
    final BinningGrid baseGrid;

    public SeadasBinningGrid(BinningGrid baseGrid) {

        if (baseGrid.getNumBins() > MAX_NUM_BINS) {
            throw new IllegalArgumentException("Base grid has more than " + MAX_NUM_BINS + " bins");
        }

        this.baseGrid = baseGrid;
    }

    public BinningGrid getBaseGrid() {
        return baseGrid;
    }

    public int getNumRows() {
        return baseGrid.getNumRows();
    }

    public int getSeadasBinIndex(long bin) {

        int row1 = baseGrid.getRowIndex(bin);
        long firstBin1 = baseGrid.getFirstBinIndex(row1);
        long col = bin - firstBin1;

        int row2 = baseGrid.getNumRows() - (row1 + 1);
        long firstBin2 = baseGrid.getFirstBinIndex(row2);

        // SeaDAS uses FORTRAN-style, 1-based indexes
        return (int) (firstBin2 + col + 1L);
    }

    public int getSeadasRowIndex(int row) {
        // SeaDAS uses FORTRAN-style, 1-based indexes
        return baseGrid.getNumRows() - (row + 1) + 1;
    }
}
