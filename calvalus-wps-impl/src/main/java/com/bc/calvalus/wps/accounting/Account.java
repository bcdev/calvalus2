package com.bc.calvalus.wps.accounting;

import com.bc.calvalus.wps.accounting.AccountBuilder;

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
        return this.platform;
    }

    public String getUsername() {
        return this.username;
    }

    public String getRef() {
        return this.ref;
    }
}