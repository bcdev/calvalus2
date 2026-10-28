package com.bc.calvalus.ogcapiprocesses.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum OGCTransmissionMode {
    @JsonProperty("value")
    VALUE,
    @JsonProperty("reference")
    REFERENCE,
}
