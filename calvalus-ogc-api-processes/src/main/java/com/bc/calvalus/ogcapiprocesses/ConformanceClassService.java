package com.bc.calvalus.ogcapiprocesses;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.ogcapiprocesses.model.OGCConfClasses;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.awt.*;
import java.util.ArrayList;
import java.util.logging.Logger;

@Path("/conformance")
public class ConformanceClassService {
    private static Logger LOG = CalvalusLogger.getLogger();
    public static final String CONF_CLASS_CORE = "http://www.opengis.net/spec/ogcapi-processes-1/1.0/req/core";
    public static final String CONF_CLASS_OGC_PROCESS_DESCRIPTION = "http://www.opengis.net/spec/ogcapi-processes-1/1.0/req/ogc-process-description";

    @GET
    @Produces({MediaType.APPLICATION_JSON})
    public Response getConformance() {

        OGCConfClasses conformance = new OGCConfClasses();
        conformance.conformsTo = new ArrayList<>();
        conformance.conformsTo.add(CONF_CLASS_CORE);
        conformance.conformsTo.add(CONF_CLASS_OGC_PROCESS_DESCRIPTION);
        return Response.ok(conformance).build();
    }
}
