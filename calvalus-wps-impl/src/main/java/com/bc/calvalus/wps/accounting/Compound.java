package com.bc.calvalus.wps.accounting;

import com.bc.calvalus.wps.accounting.CompoundBuilder;

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
        return this.id;
    }

    public String getName() {
        return this.name;
    }

    public String getType() {
        return this.type;
    }

    public String getUri() {
        return this.uri;
    }

    public String getTimestamp() {
        return this.timestamp;
    }
}
