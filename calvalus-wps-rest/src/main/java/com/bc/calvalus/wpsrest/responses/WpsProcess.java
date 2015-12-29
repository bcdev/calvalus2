package com.bc.calvalus.wpsrest.responses;

import static com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;

/**
 * @author hans
 */
public interface WpsProcess {

    String getIdentifier();

    String getTitle();

    String getAbstractText();

    String getVersion();

    ParameterDescriptor[] getParameterDescriptors();

    String getDefaultParameters();

    String[] getInputProductTypes();
}
