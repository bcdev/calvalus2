package com.bc.calvalus.binning;

/**
 * An observation of a number of measurements at a certain point in time and space.
 */
public interface Observation {
    // long getTime();
    double getLat();
    double getLon();
}
