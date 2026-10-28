package com.bc.calvalus.ogcapiprocesses.model;

import java.util.List;
import java.util.Map;

public class CalvalusProcessorDescriptor {
    public String title;
    public String description;
    public List<String> authorization;
    public List<String> inputProductTypes;
    public String outputProductTypes;
    public List<String> outputFormats;
    public Map<String, CalvalusParameterDescriptor> parameterDescriptor;
    public Map<String, CalvalusBandDescriptor> outputBandDescriptor;
}
