package com.bc.calvalus.ogcapiprocesses.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * OGC InputDescription according to
 * <a href="https://schemas.opengis.net/ogcapi/processes/part1/1.0/openapi/schemas/inputDescription.yaml">...</a>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OGCInputDescription extends OGCDescriptionType {
    public static final String UNBOUNDED = "unbounded";

    public Integer minOccurs = null;

    /** Integer or {@link #UNBOUNDED}. Defaults to 1 when omitted */
    public Object maxOccurs = null;

     /** required */
    public JsonNode schema;
}
