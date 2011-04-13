package com.bc.calvalus.binning;

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
    long getBinIndex(double lat, double lon);

    /**
     * Gets the row index for the given bin index. The pseudo code for the implementation is
     * <pre>
     *       int row = numRows - 1;
     *       while (idx < baseBin[row]) {
     *            row--;
     *       }
     *       return row;
     * </pre>
     *
     *
     * @param bin The bin index.
     * @return The row index.
     */
    int getRowIndex(long bin);

    /**
     * Gets the total number of bins in the binning grid.
     *
     * @return The total number of bins.
     */
    long getNumBins();

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

    /**
     * Gets geographical longitude and latitude (in this order) for the center of the given bin.
     *
     * @param bin The bin index.
     * @return longitude and latitude (in this order)
     */
    double[] getCenterLonLat(long bin);
}
