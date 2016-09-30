package com.bc.calvalus.wps.calvalusfacade;

import static com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;

import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;

/**
 * @author hans
 */
public class CalvalusProcessor implements WpsProcess {

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

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getTitle() {
        return title;
    }

    @Override
    public String getAbstractText() {
        return abstractText;
    }

    @Override
    public String getVersion() {
        return processorDescriptor.getProcessorVersion();
    }

    @Override
    public boolean isLocal() {
        return false;
    }

    @Override
    public ParameterDescriptor[] getParameterDescriptors() {
        return processorDescriptor.getParameterDescriptors();
    }

    /**
     * This is triggered when the processor parameter is not in XML format (eg. polymer)
     *
     * @return A template of processor parameters in a plain text format.
     */
    @Override
    public String getDefaultParameters() {
        return processorDescriptor.getDefaultParameters();
    }

    @Override
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
