package com.bc.calvalus.production;

import com.bc.ceres.core.Assert;

import java.util.Arrays;

public class ProcessorDescriptor {
    private String executableName;
    private String processorName;
    private String[] defaultParameters;  // todo - array --> scalar
    private String bundleName;
    private String[] bundleVersions;     // todo - array --> scalar

    // todo - add following fields

    // // Short description in XHTML
    // String descriptionHtml;

    // // List of output variables
    // Variable[] outputVariables;

    // // List of output variables
    // String[] inputProductTypes;

    public ProcessorDescriptor(String executableName,
                               String processorName,
                               String defaultParameters,
                               String bundleName,
                               String[] bundleVersions) {
        this(executableName,
             processorName,
             duplicate(defaultParameters, bundleVersions.length),
             bundleName, bundleVersions);
    }

    public ProcessorDescriptor(String executableName,
                               String processorName,
                               String[] defaultParameters,
                               String bundleName,
                               String[] bundleVersions) {

        Assert.notNull(executableName, "executableName");
        Assert.notNull(processorName, "processorName");
        Assert.notNull(defaultParameters, "defaultParameters");
        Assert.notNull(bundleName, "bundleName");
        Assert.notNull(bundleVersions, "bundleVersions");
        Assert.argument(defaultParameters.length == bundleVersions.length);
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

    private static String[] duplicate(String defaultParameters, int n) {
        String[] strings = new String[n];
        Arrays.fill(strings, defaultParameters);
        return strings;
    }
}
