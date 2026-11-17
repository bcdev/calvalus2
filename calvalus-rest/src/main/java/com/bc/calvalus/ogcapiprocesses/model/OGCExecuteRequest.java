package com.bc.calvalus.ogcapiprocesses.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

/**
 * OGC execute request according to
 * <a href="http://schemas.opengis.net/ogcapi/processes/part1/1.0/openapi/schemas/execute.yaml">...</a>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OGCExecuteRequest {
    public Map<String, List<JsonNode>> inputs;
    public Map<String, List<OGCOutput>> outputs;
    public OGCResponseType response;
    // optional: add support for Subscribers
}