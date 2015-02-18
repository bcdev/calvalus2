package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.GeoPos;

import java.util.Date;

/**
 * A record comprises a coordinate and an array of attribute values for each attribute described in the {@link Header}.
 *
 * @author Norman
 */
public interface Record {

    /**
     * @return The id of this record,
     *         must be unique within a single {@link com.bc.calvalus.processing.ma.RecordSource}.
     */
    int getId();

    /**
     * @return The location as (lat,lon) point or {@code null} if the location is not available (see {@link Header#hasLocation()}).
     *         The location is usually represented in form of one or more attribute values.
     *         This is the location of the corresponding reference record.
     */
    GeoPos getLocation();

    /**
     * @return The UTC time in milliseconds or {@code null} if the time is not available (see {@link Header#hasTime()}).
     *         This is the time of the corresponding reference record.
     */
    Date getTime();

    /**
     * @return The attribute values according to {@link Header#getAttributeNames()}.
     *         The array will be empty if this record doesn't have any attributes.
     */
    Object[] getAttributeValues();

    /**
     * @return The annotation values according to {@link Header#getAnnotationNames()}.
     *         The array will be empty if this record doesn't have any annotations.
     */
    Object[] getAnnotationValues();

}
