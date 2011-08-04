package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.GeoPos;

/**
 * A record comprises a coordinate and an array of attribute values for each attribute described in the header.
 *
 * @author Norman
 */
public interface Record {
    /**
     * @return The header of the record source from which this record originates.
     */
    Header getHeader();

    /**
     * @return The coordinate.
     */
    GeoPos getCoordinate();

    /**
     * @return The attribute values according to {@link Header#getAttributeNames()}.
     */
    Object[] getAttributeValues();
}
