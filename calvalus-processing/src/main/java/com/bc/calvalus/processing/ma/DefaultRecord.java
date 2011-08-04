package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.GeoPos;

/**
 * A default implementation of a {@link Record}.
 *
 * @author MarcoZ
 * @author Norman
 */
public class DefaultRecord implements Record {
    private final GeoPos coordinate;
    private final Object[] values;

    public DefaultRecord(GeoPos coordinate, Object... values) {
        this.coordinate = coordinate;
        this.values = values;
    }

    @Override
    public GeoPos getCoordinate() {
        return coordinate;
    }

    @Override
    public Object[] getAttributeValues() {
        return values;
    }
}
