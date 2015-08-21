package com.bc.calvalus.wps2;

import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;

/**
 * Created by hans on 13/08/2015.
 */
public class Processor {

    private final String identifier;
    private final String title;
    private final String abstractText;
    private final BundleDescriptor bundleDescriptor;
    private final ProcessorDescriptor processorDescriptor;

    public Processor(BundleDescriptor bundleDescriptor, ProcessorDescriptor processorDescriptor) {
        this.bundleDescriptor = bundleDescriptor;
        this.processorDescriptor = processorDescriptor;
        this.identifier = constructIdentifier();
        this.title = extractTitle();
        this.abstractText = extractAbstractText();
    }

    private String extractTitle() {
        return processorDescriptor.getProcessorName();
    }

    private String extractAbstractText() {
        return processorDescriptor.getDescriptionHtml();
    }

    private String constructIdentifier() {
        return bundleDescriptor.getBundleName() + " " + bundleDescriptor.getBundleVersion() + " " + processorDescriptor.getExecutableName();
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
}
