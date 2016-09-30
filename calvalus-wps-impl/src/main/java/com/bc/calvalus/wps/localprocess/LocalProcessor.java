package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.wps.calvalusfacade.WpsProcess;

/**
 * @author hans
 */
public class LocalProcessor implements WpsProcess {

    private static final String DELIMITER = "~";

    private final String identifier;
    private final String title;
    private final String abstractText;
    private final BundleDescriptor bundleDescriptor;
    private final ProcessorDescriptor processorDescriptor;

    public LocalProcessor(BundleDescriptor bundleDescriptor, ProcessorDescriptor processorDescriptor) {
        this.bundleDescriptor = bundleDescriptor;
        this.processorDescriptor = processorDescriptor;
        this.identifier = constructIdentifier();
        this.title = extractTitle();
        this.abstractText = extractAbstractText();
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
        return true;
    }

    @Override
    public ProcessorDescriptor.ParameterDescriptor[] getParameterDescriptors() {
        return processorDescriptor.getParameterDescriptors();
    }

    @Override
    public String getDefaultParameters() {
        return processorDescriptor.getDefaultParameters();
    }

    @Override
    public String[] getPossibleOutputFormats() {
        return processorDescriptor.getOutputFormats();
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
