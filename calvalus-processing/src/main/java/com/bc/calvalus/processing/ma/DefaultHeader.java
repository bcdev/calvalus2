package com.bc.calvalus.processing.ma;

import java.text.DateFormat;
import java.util.Arrays;
import java.util.List;

/**
 * A default implementation of a {@link RecordSource}.
 * Its main purpose is testing.
 *
 * @author Norman
 */
public class DefaultHeader implements Header {

    private List<String> attributeNames;
    private int longitudeIndex;
    private int latitudeIndex;
    private int timeIndex;
    private DateFormat timeFormat;

    public DefaultHeader(String... attributeNames) {
        this.attributeNames = Arrays.asList(attributeNames);
        this.longitudeIndex = this.attributeNames.indexOf("longitude");
        this.latitudeIndex = this.attributeNames.indexOf("latitude");
        this.timeIndex = this.attributeNames.indexOf("time");
        this.timeFormat = null;
    }

    @Override
    public String[] getAttributeNames() {
        return attributeNames.toArray(new String[attributeNames.size()]);
    }

    @Override
    public int getLongitudeIndex() {
        return longitudeIndex;
    }

    public void setLongitudeIndex(int longitudeIndex) {
        this.longitudeIndex = longitudeIndex;
    }

    @Override
    public int getLatitudeIndex() {
        return latitudeIndex;
    }

    public void setLatitudeIndex(int latitudeIndex) {
        this.latitudeIndex = latitudeIndex;
    }

    @Override
    public int getTimeIndex() {
        return timeIndex;
    }

    public void setTimeIndex(int timeIndex) {
        this.timeIndex = timeIndex;
    }

    @Override
    public DateFormat getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(DateFormat timeFormat) {
        this.timeFormat = timeFormat;
    }
}
