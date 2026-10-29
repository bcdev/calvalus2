package com.bc.calvalus.ogcapiprocesses.model;

import java.util.List;

/**
 * Landing page for an OGC API processes service as defined in
 * <a href="http://schemas.opengis.net/ogcapi/processes/part1/1.0/openapi/schemas/landingPage.yaml">...</a>
 */
public class OGCLandingPage {
    public String title;
    public String description;

    /** required */
    public List<OGCLink> links;
}
