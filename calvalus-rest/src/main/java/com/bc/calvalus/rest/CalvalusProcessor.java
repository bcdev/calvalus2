package com.bc.calvalus.rest;

import static com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;

import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;

import java.util.Map;

/**
 * @author hans
 */
public class CalvalusProcessor {

    public static final String DELIMITER = "~";

    private final String identifier;
    private final String title;
    private final String abstractText;
    private final BundleDescriptor bundleDescriptor;
    private final ProcessorDescriptor processorDescriptor;

    private String defaultCalvalusBundle;
    private String defaultSnapBundle;

    public CalvalusProcessor(BundleDescriptor bundleDescriptor, ProcessorDescriptor processorDescriptor) {
        this.bundleDescriptor = bundleDescriptor;
        this.processorDescriptor = processorDescriptor;
        this.identifier = constructIdentifier();
        this.title = extractTitle();
        this.abstractText = extractAbstractText();
        this.defaultCalvalusBundle = processorDescriptor.getJobConfiguration().get("calvalus.calvalus.bundle");
        this.defaultSnapBundle = processorDescriptor.getJobConfiguration().get("calvalus.snap.bundle");
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

    public String getVersion() {
        return processorDescriptor.getProcessorVersion();
    }

    public boolean isLocal() {
        return false;
    }

    public ParameterDescriptor[] getParameterDescriptors() {
        return processorDescriptor.getParameterDescriptors();
    }

    public Map<String,String> getJobConfiguration() {
        return processorDescriptor.getJobConfiguration();
    }

    /**
     * This is triggered when the processor parameter is not in XML format (eg. polymer)
     *
     * @return A template of processor parameters in a plain text format.
     */
    public String getDefaultParameters() {
        return processorDescriptor.getDefaultParameters();
    }

    public String[] getPossibleOutputFormats() {
        return processorDescriptor.getOutputFormats();
    }

    public String getDefaultCalvalusBundle() {
        return defaultCalvalusBundle;
    }

    public String getDefaultSnapBundle() {
        return defaultSnapBundle;
    }

    public String getBundleName() {
        return bundleDescriptor.getBundleName();
    }

    public String getBundleLocation() {
        return bundleDescriptor.getBundleLocation();
    }

    public String getBundleVersion() {
        return bundleDescriptor.getBundleVersion();
    }

    public String getName() {
        return processorDescriptor.getExecutableName();
    }

    public String[] getInputProductTypes() {
        return processorDescriptor.getInputProductTypes();
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
