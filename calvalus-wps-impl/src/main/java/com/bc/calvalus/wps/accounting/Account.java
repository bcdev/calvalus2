package com.bc.calvalus.wps.accounting;

/**
 * @author hans
 */
public class Account {
    private String platform;
    private String username;
    private String ref;

    public Account(AccountBuilder builder) {
        this.platform = builder.getPlatform();
        this.username = builder.getUsername();
        this.ref = builder.getRef();
    }

    public String getPlatform() {
        return platform;
    }

    public String getUsername() {
        return username;
    }

    public String getRef() {
        return ref;
    }
}
