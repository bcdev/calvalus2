package com.bc.calvalus.ogcapiprocesses.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class CalvalusParameterDescriptor {
    public String title;
    public String type;

    @JsonProperty("default")
    public String defaultValue;

    /** optional, for enums */
    public List<String> values;
}
