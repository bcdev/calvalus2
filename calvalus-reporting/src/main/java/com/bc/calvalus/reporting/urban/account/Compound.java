package com.bc.calvalus.reporting.urban.account;

/**
 * @author muhammad.bc.
 */
public class Compound {
    private String id;
    private String name;
    private String type;
    private Any any;

    public Compound() {}

    public Compound(String id, String name, String type, Any any) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.any = any;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public Any getAny() {
        return any;
    }
}
