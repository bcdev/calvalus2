package com.bc.calvalus.ogcapiprocesses.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CalvalusFullProcessorDescriptor {
    public CalvalusProcessorDescriptor processorDescriptor;
}
