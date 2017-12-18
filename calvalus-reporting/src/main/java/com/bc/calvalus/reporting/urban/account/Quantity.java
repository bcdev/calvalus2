package com.bc.calvalus.reporting.urban.account;

public class Quantity {
    private String id;
    private Long value;

    public Quantity() {}

    public Quantity(String id, long value) {
        this.id = id;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public Long getValue() {
        return value;
    }
}