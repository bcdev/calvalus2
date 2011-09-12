package com.bc.calvalus.processing;

import com.bc.ceres.core.Assert;
import org.esa.beam.framework.gpf.annotations.Parameter;

/**
 * Class containing information about a software bundle.
 */
public class BundleDescriptor {

    @Parameter
    private String bundleName;
    @Parameter
    private String bundleVersion;
    @Parameter(itemAlias = "processorDescriptor")
    ProcessorDescriptor[] processorDescriptors;


    public BundleDescriptor() {
    }

    public BundleDescriptor(String bundleName, String bundleVersion, ProcessorDescriptor... processorDescriptors) {
        Assert.notNull(bundleName, "bundleName");
        Assert.notNull(bundleVersion, "bundleVersion");
        this.bundleName = bundleName;
        this.bundleVersion = bundleVersion;
        this.processorDescriptors = processorDescriptors;
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
}
