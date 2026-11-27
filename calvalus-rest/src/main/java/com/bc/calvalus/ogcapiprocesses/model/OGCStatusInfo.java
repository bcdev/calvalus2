package com.bc.calvalus.ogcapiprocesses.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class OGCStatusInfo {
    public String processID;
    // single allowed value
    public String type = "process";
    public String jobID;
    public OGCJobStatus status;
}
