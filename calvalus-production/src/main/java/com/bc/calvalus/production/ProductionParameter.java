package com.bc.calvalus.production;

/**
* A production parameter (key-value pair).
*
* @author Norman
*/
public class ProductionParameter {
    private String name;
    private String value;

    public ProductionParameter(String name, String value) {
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (name.isEmpty()) {
            throw new IllegalArgumentException("name.isEmpty()");
        }
        if (value == null) {
            throw new NullPointerException("value");
        }
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }
}
