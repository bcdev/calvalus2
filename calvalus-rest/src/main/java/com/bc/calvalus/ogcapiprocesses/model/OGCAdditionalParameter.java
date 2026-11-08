package com.bc.calvalus.ogcapiprocesses.model;

import java.util.List;

/**
 * OGC AdditionalParameter according to
 * <a href="https://schemas.opengis.net/ogcapi/processes/part1/1.0/openapi/schemas/additionalParameter.yaml">...</a>
 */
public class OGCAdditionalParameter {
    /** required */
    public String name;

    /** Required. Items may be String, Number, Array or Object. */
    public List<Object> value;
}
