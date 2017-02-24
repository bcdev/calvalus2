package com.bc.calvalus.extractor;

/**
 * @author muhammad.bc.
 */
public enum ReadFormatType {
    XML("application/xml"), JSON("application/json");

    private final String format;

    ReadFormatType(String format) {
        this.format = format;
    }

    public String getFormat() {
        return format;
    }
}
