package com.bc.calvalus.ogcapiprocesses.model;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * OGC Output Format according to
 * <a href="http://schemas.opengis.net/ogcapi/processes/part1/1.0/openapi/schemas/format.yaml">...</a>
 */
public class OGCFormat {
    public String mediaType;
    public String encoding;
    public JsonNode schema;
}
