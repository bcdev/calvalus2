package com.bc.calvalus.generator.extractor;

/**
 * @author muhammad.bc.
 */
public enum FormatType {
    XML("application/xml"), JSON("application/json");

    private final String format;

    FormatType(String format) {
        this.format = format;
    }

    public String getFormat() {
        return format;
    }
}
