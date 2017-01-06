package com.bc.calvalus.generator.reader;

/**
 * @author muhammad.bc.
 */
public enum FormatType {
    XML("application/xml"), JSON("application/json");

    private String format;

    FormatType(String format) {
        this.format = format;
    }

    public String getFormat() {
        return format;
    }
}
