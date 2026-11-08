package com.bc.calvalus.ogcapiprocesses.model;

import java.util.Map;

/**
 * Process Description following
 * <a href="https://schemas.opengis.net/ogcapi/processes/part1/1.0/openapi/schemas/process.yaml">...</a>
 */
public class OGCProcessDescription extends OGCProcessSummary {
    public Map<String, OGCInputDescription> inputs;
    public Map<String, OGCOutputDescription> outputs;
}