package com.bc.calvalus.ogcapiprocesses;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.ogcapiprocesses.model.OGCLandingPage;
import com.bc.calvalus.ogcapiprocesses.model.OGCLink;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.logging.Logger;

@Path("/")
public class LandingPageService {
    private static Logger LOG = CalvalusLogger.getLogger();
    public static final String CONFORMACE_REL_TYPE = "http://www.opengis.net/def/rel/ogc/1.0/conformance";
    public static final String PROCESSED_REL_TYPE = "http://www.opengis.net/def/rel/ogc/1.0/processes";
    public static final String JSON_MIME = "application/json";

    @GET
    @Produces({MediaType.APPLICATION_JSON})
    public Response getLandingPage(
            @Context UriInfo uriInfo
        ) {
        OGCLandingPage landingPage = new OGCLandingPage();
        landingPage.title = "Calvalus OGC API Processes server";
        landingPage.description = "Web server implementing OGC API - Processes 1.0 Standard for Calvalus";
        landingPage.links = new ArrayList<>();

        OGCLink conformanceLink = new OGCLink();
        conformanceLink.href = uriInfo.getBaseUriBuilder().path("conformance").build().toString();
        conformanceLink.rel = CONFORMACE_REL_TYPE;
        conformanceLink.type = JSON_MIME;
        conformanceLink.title = "OGC API - Processes conformance classes implemented by this server";

        OGCLink processesLink = new OGCLink();
        processesLink.href = uriInfo.getBaseUriBuilder().path("processes").build().toString();
        processesLink.rel = PROCESSED_REL_TYPE;
        processesLink.type = JSON_MIME;
        processesLink.title = "List of executable processes with metadata";

        OGCLink selfLink = OGCLink.self(uriInfo.getAbsolutePath().toString());
        selfLink.title = "This document";

        landingPage.links.add(selfLink);
        landingPage.links.add(conformanceLink);
        landingPage.links.add(processesLink);
        return Response.ok(landingPage).build();
    }
}
