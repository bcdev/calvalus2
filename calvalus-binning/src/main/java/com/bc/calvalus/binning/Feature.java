package com.bc.calvalus.binning;

/**
 * A named statistics feature.
 *
 * @author Norman
 */
public interface Feature {
    /**
     * @return The feature's name.
     */
    String getName();

    /**
     * @return The feature's fill value.
     */
    float getFillValue();
}
