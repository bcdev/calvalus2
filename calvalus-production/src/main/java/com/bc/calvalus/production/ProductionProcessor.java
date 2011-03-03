package com.bc.calvalus.production;

public class ProductionProcessor {
    private String executableName;
    private String processorName;
    private String defaultParameters;
    private String bundleName;
    private String[] bundleVersions;

    public ProductionProcessor(String executableName,
                               String processorName,
                               String defaultParameters,
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
