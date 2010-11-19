package com.bc.calvalus.binning;

/**
 * A bin in which observations (a pixel's sample values) are collected.
 * @param <OBS> The observation type.
 */
public interface Bin<OBS extends Observation> {
    int getIndex();

    void addObservation(OBS observation);

    void close();
}
