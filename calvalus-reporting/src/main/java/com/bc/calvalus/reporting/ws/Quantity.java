package com.bc.calvalus.reporting.ws;

/**
 * @author hans
 */
public class Quantity {

    private String id;
    private long value;

    Quantity(String id, long value) {
        this.id = id;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public long getValue() {
        return value;
    }
}
