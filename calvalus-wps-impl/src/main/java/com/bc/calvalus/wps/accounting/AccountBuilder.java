package com.bc.calvalus.wps.accounting;

import com.bc.calvalus.wps.accounting.Account;

/**
 * @author hans
 */
public class AccountBuilder {
    private String platform;
    private String username;
    private String ref;

    private AccountBuilder() {
    }

    public static AccountBuilder create() {
        return new AccountBuilder();
    }

    public Account build() {
        return new Account(this);
    }

    public AccountBuilder withPlatform(String platform) {
        this.platform = platform;
        return this;
    }

    public AccountBuilder withUsername(String username) {
        this.username = username;
        return this;
    }

    public AccountBuilder withRef(String ref) {
        this.ref = ref;
        return this;
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
