package com.bc.calvalus.wps.accounting;

/**
 * @author hans
 */
public class Compound {

    private String id;
    private String name;
    private String type;
    private String uri;
    private String timestamp;

    public Compound(CompoundBuilder builder) {
        this.id = builder.getId();
        this.name = builder.getName();
        this.type = builder.getType();
        this.uri = builder.getUri();
        this.timestamp = builder.getTimestamp();
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

    public String getUri() {
        return uri;
    }

    public String getTimestamp() {
        return timestamp;
    }
}
