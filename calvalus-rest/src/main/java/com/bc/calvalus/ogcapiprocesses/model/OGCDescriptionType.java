package com.bc.calvalus.ogcapiprocesses.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * OGC Description Type according to
 * <a href="https://schemas.opengis.net/ogcapi/processes/part1/1.0/openapi/schemas/descriptionType.yaml">...</a>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class OGCDescriptionType {
    public String title;
    public String description;
    public List<String> keywords;
    public List<OGCMetadata> metadata;
    public OGCAdditionalParameters additionalParameters;
}
