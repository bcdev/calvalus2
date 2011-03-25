package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

import java.util.Arrays;

/**
 * GWT-serializable version of the {@link com.bc.calvalus.production.ProcessorDescriptor} class.
 *
 * @author Norman
 */
public class GsProcessorDescriptor implements IsSerializable {
    private String executableName;
    private String processorName;
    private String[] defaultParameters;
    private String bundleName;
    private String[] bundleVersions;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public GsProcessorDescriptor() {
    }

    public GsProcessorDescriptor(String executableName,
                                 String processorName,
                                 String[] defaultParameters,
                                 String bundleName,
                                 String[] bundleVersions) {
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

    public String[] getDefaultParameters() {
        return defaultParameters;
    }

    public String getBundleName() {
        return bundleName;
    }

    public String[] getBundleVersions() {
        return bundleVersions;
    }
}
