package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.GeoPos;

/**
 * A match-up record.
 *
 * @author Norman
 */
public class DefaultRecord implements Record{
    private final Header header;
    private final GeoPos coordinate;
    private final Object[] values;

    public DefaultRecord(Header header, GeoPos coordinate, Object ... values) {
        this.header = header;
        this.coordinate = coordinate;
        this.values = values;
    }

    @Override
    public Header getHeader() {
        return header;
    }

    @Override
    public GeoPos getCoordinate() {
        return coordinate;
    }

    @Override
    public Object[] getValues() {
        return values;
    }
}
