package com.bc.calvalus.binning;

/**
 * An observation comprises a number of measurements at a certain point in time and space.
 */
public interface Observation {
    // long getTime();
    double getLat();
    double getLon();

    // todo - model measurements
    // int getNumVariables();
    // double getMeasurement(int varIndex);
    // todo - model variable descriptor (name, units, is log distr., avg. method)
    // Variable[] getVariables();
}
