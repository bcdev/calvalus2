package com.bc.calvalus.ogcapiprocesses.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum OGCJobStatus {
    @JsonProperty("accepted")
    ACCEPTED,
    @JsonProperty("running")
    RUNNING,
    @JsonProperty("successful")
    SUCCESSFUL,
    @JsonProperty("failed")
    FAILED,
    @JsonProperty("dismissed")
    DISMISSED,
}
