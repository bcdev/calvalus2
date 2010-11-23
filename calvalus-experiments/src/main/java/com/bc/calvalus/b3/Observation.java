package com.bc.calvalus.b3;

/**
 * An observation comprises a number of measurements at a certain point in time and space.
 *
 * @author Norman Fomferra
 */
public interface Observation extends Vector {
    /**
     * @return The time of this observation given as Modified Julian Day (MJD).
     */
    double getMJD();

    /**
     * @return The geographical latitude of this observation given as WGS-84 coordinate.
     */
    double getLatitude();

    /**
     * @return The geographical longitude of this observation given as WGS-84 coordinate.
     */
    double getLongitude();
}
