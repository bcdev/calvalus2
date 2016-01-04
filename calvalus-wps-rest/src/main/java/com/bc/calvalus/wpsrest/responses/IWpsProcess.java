package com.bc.calvalus.wpsrest.responses;

import static com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;

/**
 * @author hans
 */
public interface IWpsProcess {

    String getIdentifier();

    String getTitle();

    String getAbstractText();

    String getVersion();

}
