package com.bc.calvalus.ogcapiprocesses.model;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * OGC InputDescription according to
 * <a href="https://schemas.opengis.net/ogcapi/processes/part1/1.0/openapi/schemas/inputDescription.yaml">...</a>
 */
public class OGCInputDescription extends OGCDescriptionType {
    public static final String UNBOUNDED = "unbounded";

    public Integer minOccurs = 1;

    /** Integer or {@link #UNBOUNDED}. Defaults to 1 when omitted */
    public Object maxOccurs = 1;

     /** required */
    public JsonNode schema;
}
