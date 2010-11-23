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
}
