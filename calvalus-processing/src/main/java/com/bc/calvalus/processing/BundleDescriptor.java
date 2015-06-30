package com.bc.calvalus.processing;

import com.bc.ceres.core.Assert;
import org.esa.snap.framework.gpf.annotations.Parameter;

/**
 * Class containing information about a software bundle.
 */
public class BundleDescriptor {

    @Parameter
    private String bundleName;
    @Parameter
    private String bundleVersion;
    @Parameter
    private String includeBundle;
    @Parameter(itemAlias = "processorDescriptor")
    ProcessorDescriptor[] processorDescriptors;
    @Parameter(itemAlias = "aggregatorDescriptor")
    AggregatorDescriptor[] aggregatorDescriptors;

    private String bundleLocation;


    public BundleDescriptor() {
    }

    public BundleDescriptor(String bundleName, String bundleVersion, String bundleLocation, ProcessorDescriptor... processorDescriptors) {
        Assert.notNull(bundleName, "bundleName");
        Assert.notNull(bundleVersion, "bundleVersion");
        this.bundleName = bundleName;
        this.bundleVersion = bundleVersion;
        this.processorDescriptors = processorDescriptors;
        this.bundleLocation = bundleLocation;
    }

    public String getBundleName() {
        return bundleName;
    }

    public String getBundleVersion() {
        return bundleVersion;
    }

    public ProcessorDescriptor[] getProcessorDescriptors() {
        return processorDescriptors;
    }

    public AggregatorDescriptor[] getAggregatorDescriptors() {
        return aggregatorDescriptors;
    }

    public void setBundleLocation(String bundleLocation) {
        this.bundleLocation = bundleLocation;
    }

    public String getBundleLocation() {
        return bundleLocation;
    }

    public String getIncludeBundle() {
        return includeBundle;
    }
}
