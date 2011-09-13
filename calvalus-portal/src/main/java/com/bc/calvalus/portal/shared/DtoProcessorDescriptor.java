package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * GWT-serializable version of the {@link com.bc.calvalus.processing.ProcessorDescriptor} class.
 *
 * @author Norman
 */
public class DtoProcessorDescriptor implements IsSerializable {
    private String executableName;
    private String processorName;
    private String processorVersion;
    private String defaultParameter;
    private String bundleName;
    private String bundleVersion;
    private String descriptionHtml;
    private String defaultMaskExpression;
    private DtoProcessorVariable[] processorVariables;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public DtoProcessorDescriptor() {
    }

    public DtoProcessorDescriptor(String executableName,
                                  String processorName,
                                  String processorVersion,
                                  String defaultParameter,
                                  String bundleName,
                                  String bundleVersion,
                                  String descriptionHtml,
                                  String defaultMaskExpression,
                                  DtoProcessorVariable[] processorVariables) {
        this.executableName = executableName;
        this.processorName = processorName;
        this.processorVersion = processorVersion;
        this.defaultParameter = defaultParameter;
        this.bundleName = bundleName;
        this.bundleVersion = bundleVersion;
        this.descriptionHtml = descriptionHtml;
        this.defaultMaskExpression = defaultMaskExpression;
        this.processorVariables = processorVariables;
    }

    public String getExecutableName() {
        return executableName;
    }

    public String getProcessorName() {
        return processorName;
    }

    public String getProcessorVersion() {
        return processorVersion;
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

    public DtoProcessorVariable[] getProcessorVariables() {
        return processorVariables;
    }

    public String getDefaultMaskExpr() {
        return defaultMaskExpression;
    }
}
