package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

public class PortalProcessor implements IsSerializable {
    private String executableName;
    private String processorName;
    private String defaultParameters;
    private String bundleName;
    private String[] bundleVersions;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public PortalProcessor() {
    }

    public PortalProcessor(String executableName, String processorName, String defaultParameters, String bundleName, String[] bundleVersions) {
        this.executableName = executableName;
        this.processorName = processorName;
        this.defaultParameters = defaultParameters;
        this.bundleName = bundleName;
        this.bundleVersions = bundleVersions;
    }

    public String getExecutableName() {
        return executableName;
    }

    public String getProcessorName() {
        return processorName;
    }

    public String getDefaultParameters() {
        return defaultParameters;
    }

    public String getBundleName() {
        return bundleName;
    }

    public String[] getBundleVersions() {
        return bundleVersions;
    }
}
