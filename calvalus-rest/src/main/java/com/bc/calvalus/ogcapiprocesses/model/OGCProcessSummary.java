package com.bc.calvalus.ogcapiprocesses.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.List;


/**
 * OGC ProcessSummary (part of ProcessDescription) according to
 * <a href="https://schemas.opengis.net/ogcapi/processes/part1/1.0/openapi/schemas/processSummary.yaml">...</a>
 */
@JsonPropertyOrder({"id", "version", "title", "description"})
public class OGCProcessSummary extends OGCDescriptionType {
    /** Required */
    public String id;
    /** Required */
    public String version;

    public List<OGCJobControlOption> jobControlOptions;
    public List<OGCTransmissionMode> outputTransmission;
    public List<OGCLink> links;
}
