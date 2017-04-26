package com.bc.calvalus.wps.accounting;

public class Quantity {
    private String id;
    private long value;

    Quantity(String id, long value) {
        this.id = id;
        this.value = value;
    }

    public String getId() {
        return this.id;
    }

    public long getValue() {
        return this.value;
    }
}
