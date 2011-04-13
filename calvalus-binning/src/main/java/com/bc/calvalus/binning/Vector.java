package com.bc.calvalus.binning;

/**
 * A vector of {@code float} elements.
 *
 * @author Norman Fomferra
 */
public interface Vector {
    /**
     * @return The size of the vector (number of elements).
     */
    int size();

    /**
     * Gets the {@code float} element at the given index.
     *
     * @param index The element index.
     * @return The {@code float} element at the given index.
     */
    float get(int index);
}
