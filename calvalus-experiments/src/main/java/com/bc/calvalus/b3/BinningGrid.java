package com.bc.calvalus.b3;

/**
 * The grid used for the binning.
 *
 * @author Norman Fomferra
 */
public interface BinningGrid {
    /**
     * Transforms a geographical point into a unique bin index.
     *
     * @param lat The latitude in degrees.
     * @param lon The longitude in degrees.
     * @return The unique bin index.
     */
    int getBinIndex(double lat, double lon);

    /**
     * Gets the row index for the given bin index.
     *
     * @param bin The bin index.
     * @return The row index.
     */
    int getRowIndex(int bin);

    /**
     * Gets the number of rows in this grid.
     *
     * @return The number of rows.
     */
    int getNumRows();

    /**
     * Gets the number of columns in the given row.
     *
     * @param row The row index.
     * @return The number of columns.
     */
    int getNumCols(int row);
}
