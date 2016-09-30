package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.processing.ProcessorDescriptor;

/**
 * @author hans
 */
public interface WpsProcess {

    String getIdentifier();

    String getTitle();

    String getAbstractText();

    String getVersion();

    boolean isLocal();

    ProcessorDescriptor.ParameterDescriptor[] getParameterDescriptors();

    String getDefaultParameters();

    String[] getPossibleOutputFormats();

}
