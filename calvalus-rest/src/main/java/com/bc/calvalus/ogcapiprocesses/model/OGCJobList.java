package com.bc.calvalus.ogcapiprocesses.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;

/**
 * OGC JobList according to <a href="https://schemas.opengis.net/ogcapi/processes/part1/1.0/openapi/schemas/jobList.yaml">...</a>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OGCJobList {
    // required
    public ArrayList<OGCStatusInfo> jobs;
    // required
    public ArrayList<OGCLink> links;
}
