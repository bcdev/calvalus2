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
    private final Date time;
    private final Object[] attributeValues;
    private final Object[] annotationValues;

    public DefaultRecord(GeoPos location, Date time, Object[] attributeValues) {
        this(location, time, attributeValues, new Object[]{""});
    }

    public DefaultRecord(GeoPos location, Date time, Object[] attributeValues, Object[] annotationValues) {
        this.location = location;
        this.time = time;
        this.attributeValues = attributeValues;
        this.annotationValues = annotationValues;
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
