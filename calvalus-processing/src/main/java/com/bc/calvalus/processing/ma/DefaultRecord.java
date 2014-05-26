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

    private final int recordId;
    private final GeoPos location;
    private final Date time;
    private final Object[] attributeValues;
    private final Object[] annotationValues;

    public DefaultRecord(int recordId, GeoPos location, Date time, Object[] attributeValues) {
        this(recordId, location, time, attributeValues, new Object[]{""});
    }

    public DefaultRecord(int recordId, GeoPos location, Date time, Object[] attributeValues, Object[] annotationValues) {
        this.recordId = recordId;
        this.location = location;
        this.time = time;
        this.attributeValues = attributeValues;
        this.annotationValues = annotationValues;
    }

    @Override
    public int getId() {
        return recordId;
    }

    @Override
    public GeoPos getLocation() {
        return location;
    }

    @Override
    public Date getTime() {
        return time;
    }

    @Override
    public Object[] getAttributeValues() {
        return attributeValues;
    }

    @Override
    public Object[] getAnnotationValues() {
        return annotationValues;
    }

    @Override
    public String toString() {
        return "DefaultRecord{" +
               "location=" + location +
               ", time=" + time +
               ", values=" + Arrays.asList(attributeValues) +
               '}';
    }
}
