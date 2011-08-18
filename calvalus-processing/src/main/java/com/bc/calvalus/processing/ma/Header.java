package com.bc.calvalus.processing.ma;

import java.text.DateFormat;

/**
 * A header is used to describe the records of a record source.
 *
 * @author MarcoZ
 * @author Norman
 */
public interface Header {
    /**
     * @return The array of attribute names.
     */
    String[] getAttributeNames();

    /**
     * @return The index of the attribute that provides the longitude (a Java {@code Number}, decimal degrees, range -180 to +180).
     * Returns {@code -1} if the attribute is missing.
     */
    int getLongitudeIndex();

    /**
     * @return The index of the attribute that provides the latitude (a Java {@code Number}, decimal degrees, range -90 to +90).
     * Returns {@code -1} if the attribute is missing.
     */
    int getLatitudeIndex();

    /**
     * @return The index of the attribute that provides the time (a Java {@code Number}, decimal MJD).
     * Returns {@code -1} if the attribute is missing.
     */
    int getTimeIndex();

    /**
     * @return The pattern used to convert time values into text. May be {@code null}.
     */
    DateFormat getTimeFormat();
}
