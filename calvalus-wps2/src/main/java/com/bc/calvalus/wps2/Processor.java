package com.bc.calvalus.wps2;

import static com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;

import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;

/**
 * Created by hans on 13/08/2015.
 */
public class Processor {

    public static final String DELIMITER = "~";

    private final String identifier;
    private final String title;
    private final String abstractText;
    private final BundleDescriptor bundleDescriptor;
    private final ProcessorDescriptor processorDescriptor;

    private String defaultCalvalusBundle;
    private String defaultBeamBundle;

    public Processor(BundleDescriptor bundleDescriptor, ProcessorDescriptor processorDescriptor) {
        this.bundleDescriptor = bundleDescriptor;
        this.processorDescriptor = processorDescriptor;
        this.identifier = constructIdentifier();
        this.title = extractTitle();
        this.abstractText = extractAbstractText();
        this.defaultCalvalusBundle = processorDescriptor.getJobConfiguration().get("calvalus.calvalus.bundle");
        this.defaultBeamBundle = processorDescriptor.getJobConfiguration().get("calvalus.beam.bundle");
    }

    public String getIdentifier() {
        return identifier;
    }

    public String getTitle() {
        return title;
    }

    public String getAbstractText() {
        return abstractText;
    }

    public ParameterDescriptor[] getParameterDescriptors() {
        return processorDescriptor.getParameterDescriptors();
    }

    public String getDefaultParameters() {
        return processorDescriptor.getDefaultParameters();
    }

    public String getDefaultCalvalusBundle() {
        return defaultCalvalusBundle;
    }

    public String getDefaultBeamBundle() {
        return defaultBeamBundle;
    }

    private String extractTitle() {
        return processorDescriptor.getProcessorName();
    }

    private String extractAbstractText() {
        return processorDescriptor.getDescriptionHtml();
    }

    private String constructIdentifier() {
        return bundleDescriptor.getBundleName() + DELIMITER + bundleDescriptor.getBundleVersion() + DELIMITER + processorDescriptor.getExecutableName();
    }

}
