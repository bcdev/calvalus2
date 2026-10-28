package com.bc.calvalus.ogcapiprocesses.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * OGC JobControlOption according to
 * <a href="http://schemas.opengis.net/ogcapi/processes/part1/1.0/openapi/schemas/jobControlOptions.yaml">...</a>
 */
public enum OGCJobControlOption {
    @JsonProperty("sync-execute")
    SYNC_EXECUTE,
    @JsonProperty("async-execute")
    ASYNC_EXECUTE,
    @JsonProperty("dismiss")
    DISMISS,
}
