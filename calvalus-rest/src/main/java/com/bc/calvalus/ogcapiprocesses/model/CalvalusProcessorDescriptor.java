package com.bc.calvalus.ogcapiprocesses.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CalvalusProcessorDescriptor {
    public String title;
    public String version;
    public String description;
    public List<String> authorization;
    public List<String> inputProductTypes;
    public String outputProductTypes;
    public List<String> outputFormats;
    public Map<String, CalvalusParameterDescriptor> parameterDescriptor;
    public Map<String, CalvalusBandDescriptor> outputBandDescriptor;
}
