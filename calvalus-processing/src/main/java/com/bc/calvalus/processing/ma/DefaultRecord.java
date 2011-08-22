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
    private final GeoPos coordinate;
    private final Date time;
    private final Object[] values;

// Currently there is no use for the following block. Remove by end of September 2011   :-)   (nf)
/*
    public static DefaultRecord create(Header header, Object... values) {

        final int latitudeIndex = header != null ? header.getLatitudeIndex() : -1;
        final int longitudeIndex = header != null ? header.getLongitudeIndex() : -1;
        final int timeIndex = header != null ? header.getTimeIndex() : -1;
        final DateFormat timeFormat = header != null ? header.getTimeFormat() : null;

        GeoPos coordinate = null;
        if (latitudeIndex >= 0 && longitudeIndex >= 0) {
            coordinate = new GeoPos(((Number) values[latitudeIndex]).floatValue(),
                                    ((Number) values[longitudeIndex]).floatValue());
        }

        Date time = null;
        if (timeIndex >= 0 && timeFormat != null) {
            final String timeStr = (String) values[timeIndex];
            if (timeStr != null) {
                try {
                    time = timeFormat.parse(timeStr);
                } catch (ParseException e) {
                    throw new IllegalArgumentException("Illegal time value: " + timeStr + ", expected format " + timeFormat);
                }
            }
        }

        return new DefaultRecord(coordinate, time, values);
    }
*/


    DefaultRecord(GeoPos coordinate, Date time, Object[] values) {
        this.coordinate = coordinate;
        this.time = time;
        this.values = values;
    }

    @Override
    public GeoPos getCoordinate() {
        return coordinate;
    }

    @Override
    public Date getTime() {
        return time;
    }

    @Override
    public Object[] getAttributeValues() {
        return values;
    }

    @Override
    public String toString() {
        return "DefaultRecord{" +
                "coordinate=" + coordinate +
                ", time=" + time +
                ", values=" + Arrays.asList(values) +
                '}';
    }
}
