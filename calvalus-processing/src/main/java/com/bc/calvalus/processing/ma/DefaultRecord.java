package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.GeoPos;

import java.util.Arrays;
import java.util.Date;

/**
 * A default implementation of a {@link Record}.
 *
 * @author MarcoZ
 * @author Norman
 */
public class DefaultRecord implements Record {
    private final GeoPos location;
    private final Date timestamp;
    private final Object[] values;

    public DefaultRecord(GeoPos location, Date timestamp, Object[] values) {
        this.location = location;
        this.timestamp = timestamp;
        this.values = values;
    }

    @Override
    public GeoPos getLocation() {
        return location;
    }

    @Override
    public Date getTimestamp() {
        return timestamp;
    }

    @Override
    public Object[] getAttributeValues() {
        return values;
    }

    @Override
    public String toString() {
        return "DefaultRecord{" +
                "location=" + location +
                ", time=" + timestamp +
                ", values=" + Arrays.asList(values) +
                '}';
    }
}
