package com.bc.calvalus.processing.ma;

/**
 * A header is used to describe the {@link Record}s provided by a {@link RecordSource}.
 *
 * @author MarcoZ
 * @author Norman
 */
public interface Header {

    /**
     * @return {@code true}, if records that conform to this header return location values (see {@link Record#getLocation()}).
     */
    boolean hasLocation();

    /**
     * @return {@code true}, if records that conform to this header return time values (see {@link Record#getTime()}).
     */
    boolean hasTime();

    /**
     * @return The array of attribute names.
     */
    String[] getAttributeNames();
}
