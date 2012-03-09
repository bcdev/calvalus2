package org.esa.beam.binning.support;

import org.esa.beam.binning.PlanetaryGrid;

/**
 * Thin wrapper around a {@code PlanetaryGrid} used to convert from BEAM row and bin indexes to the ones
 * used in SeaDAS. BEAM row and bin indexes are 0-based and increase from North to South (top down), while the
 * SeaDAS ones are 1-based and from South to North (bottom up). In both grids, columns indexes increase from East
 * to West (left to right).
 *
 * @author Norman Fomferra
 */
public class SeadasGrid {

    public static final int MAX_NUM_BINS = Integer.MAX_VALUE - 1;
    private final PlanetaryGrid baseGrid;

    public SeadasGrid(PlanetaryGrid baseGrid) {

        if (!isCompatibleBaseGrid(baseGrid)) {
            throw new IllegalArgumentException("Base grid has more than " + MAX_NUM_BINS + " bins");
        }

        this.baseGrid = baseGrid;
    }

    public static boolean isCompatibleBaseGrid(PlanetaryGrid baseGrid) {
        return baseGrid.getNumBins() <= MAX_NUM_BINS;
    }

    public int getNumRows() {
        return baseGrid.getNumRows();
    }

    public int getNumCols(int row) {
        return baseGrid.getNumCols(row);
    }

    public int getRowIndex(int row) {
        // SeaDAS uses FORTRAN-style, 1-based indexes
        return baseGrid.getNumRows() - (row + 1) + 1;
    }

    public int getBinIndex(long bin) {

        int row1 = baseGrid.getRowIndex(bin);
        long firstBin1 = baseGrid.getFirstBinIndex(row1);
        long col = bin - firstBin1;

        int row2 = baseGrid.getNumRows() - (row1 + 1);
        long firstBin2 = baseGrid.getFirstBinIndex(row2);

        // SeaDAS uses FORTRAN-style, 1-based indexes
        return (int) (firstBin2 + col + 1L);
    }


}
