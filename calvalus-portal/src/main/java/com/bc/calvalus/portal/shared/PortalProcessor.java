package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

public class PortalProcessor implements IsSerializable {
    private String operator;
    private String name;
    private String bundle;
    private String[] versions;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public PortalProcessor() {
    }

    public PortalProcessor(String operator, String name, String bundle, String[] versions) {
        this.operator = operator;
        this.name = name;
        this.bundle = bundle;
        this.versions = versions;
    }

    public String getOperator() {
        return operator;
    }

    public String getName() {
        return name;
    }

    public String getBundle() {
        return bundle;
    }

    public String[] getVersions() {
        return versions;
    }
}
