package com.bc.calvalus.ogcapiprocesses.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum OGCResponseType {
    @JsonProperty("raw") RAW,
    @JsonProperty("document") DOCUMENT,
}
