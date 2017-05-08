package com.bc.calvalus.urban.account;

/**
 * @author muhammad.bc.
 */
public class Account {
    private String platform;
    private String userName;
    private String ref;

    public Account(String platform, String userName, String ref) {
        this.platform = platform;
        this.userName = userName;
        this.ref = ref;
    }

    public String getPlatform() {
        return platform;
    }

    public String getUserName() {
        return userName;
    }

    public String getRef() {
        return ref;
    }
}
