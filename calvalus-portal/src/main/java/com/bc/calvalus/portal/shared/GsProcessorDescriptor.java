package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * GWT-serializable version of the {@link com.bc.calvalus.production.ProcessorDescriptor} class.
 *
 * @author Norman
 */
public class GsProcessorDescriptor implements IsSerializable {
    private String executableName;
    private String processorName;
    private String defaultParameter;
    private String bundleName;
    private String bundleVersion;
    private String descriptionHtml;
    private GsProcessorVariable[] processorVariables;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public GsProcessorDescriptor() {
    }

    public GsProcessorDescriptor(String executableName,
                                 String processorName,
                                 String defaultParameter,
                                 String bundleName,
                                 String bundleVersion,
                                 String descriptionHtml,
                                 GsProcessorVariable[] processorVariables) {
        this.executableName = executableName;
        this.processorName = processorName;
        this.defaultParameter = defaultParameter;
        this.bundleName = bundleName;
        this.bundleVersion = bundleVersion;
        this.descriptionHtml = descriptionHtml;
        this.processorVariables = processorVariables;
    }

    public String getExecutableName() {
        return executableName;
    }

    public String getProcessorName() {
        return processorName;
    }

    public String getDefaultParameter() {
        return defaultParameter;
    }

    public String getBundleName() {
        return bundleName;
    }

    public String getBundleVersion() {
        return bundleVersion;
    }

    public String getDescriptionHtml() {
        return descriptionHtml;
    }

    public GsProcessorVariable[] getProcessorVariables() {
        return processorVariables;
    }
}
