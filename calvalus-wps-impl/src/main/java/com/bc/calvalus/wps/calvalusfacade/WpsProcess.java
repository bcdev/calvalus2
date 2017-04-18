package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.processing.ProcessorDescriptor;

import java.util.Map;

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

    Map<String,String> getJobConfiguration();

    String getDefaultParameters();

    String[] getPossibleOutputFormats();

}
