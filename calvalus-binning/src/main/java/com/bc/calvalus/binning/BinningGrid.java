package com.bc.calvalus.binning;

/**
 * The grid used for the binning.
 */
public interface BinningGrid {
    /**
     * Transforms a geographical point into a unique grid index.
     * @param lat The latitude in degrees.
     * @param lon The longitude in degrees.
     * @return The unique grid index.
     */
    int getBinIndex(double lat, double lon);

    // double[] getCenterLatLon(int idx);
}
