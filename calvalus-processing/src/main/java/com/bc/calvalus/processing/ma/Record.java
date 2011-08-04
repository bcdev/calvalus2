package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.GeoPos;

/**
 * A record comprises a coordinate and an array of attribute values for each attribute described in the header.
 *
 * @author Norman
 */
public interface Record {

    /**
     * @return The coordinate which is usually internally represented in form of one or more attribute values.
     */
    GeoPos getCoordinate();

    /**
     * @return The attribute values according to {@link Header#getAttributeNames()}.
     */
    Object[] getAttributeValues();
}
