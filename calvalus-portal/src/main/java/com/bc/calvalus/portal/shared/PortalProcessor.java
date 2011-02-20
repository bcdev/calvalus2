package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

public class PortalProcessor implements IsSerializable {
    private String id;
    private String name;
    private String type;
    private String[] versions;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public PortalProcessor() {
    }

    public PortalProcessor(String id, String type, String name, String[] versions) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.versions = versions;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String[] getVersions() {
        return versions;
    }
}
