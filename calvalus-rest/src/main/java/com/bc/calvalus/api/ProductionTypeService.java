package com.bc.calvalus.api;

import com.bc.calvalus.commons.CalvalusLogger;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("production-types")
public class ProductionTypeService {

    private static Logger LOG = CalvalusLogger.getLogger();
    private static String PRODUCTION_TYPE_DIR = "production-types";

    @GET
    @Produces({MediaType.APPLICATION_JSON})
    public Response list(
            @QueryParam("names") String names,
            @Context HttpServletRequest request,
            @Context SecurityContext securityContext,
            @Context ServletContext context
    ) {
        try {
            final String catalinaHome = System.getProperty("catalina.home");
            final String productionTypeDirPath = catalinaHome + "/content/" + PRODUCTION_TYPE_DIR;
            final IdMatcher idMatcher = new IdMatcher(names);

            final String[] productionTypeFilenames = new File(productionTypeDirPath).list((File _file, String name) -> name.endsWith("-cht-type.json"));
            StringBuilder accu = new StringBuilder("[");
            for (String filename: productionTypeFilenames) {
                final String id = filename.substring(0, filename.length() - "-cht-type.json".length());
                if (idMatcher.matches(id)) {
                    if (accu.length() > 1) {
                        accu.append(",");
                    }
                    accu.append("\n  \"");
                    accu.append(id);
                    accu.append("\"");
                }
            }
            if (accu.length() > 1) {
                accu.append("\n]");
            } else {
                accu.append("]");
            }

            return Response.ok(String.valueOf(accu) + "\n").build();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new WebApplicationException(e,
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(e.getMessage())
                            .type(MediaType.TEXT_PLAIN)
                            .build());
        }
    }

    @GET
    @Path("/{name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    public Response show(
            @PathParam("name") String name,
            @Context HttpServletRequest request,
            @Context SecurityContext securityContext,
            @Context ServletContext context
    ) {
        try {
            final String catalinaHome = System.getProperty("catalina.home");
            final String productionTypePath = catalinaHome + "/content/production-types/" + name + "-cht-type.json";
            if (new File(productionTypePath).exists()) {

                final StringBuilder accu = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(new FileReader(productionTypePath))) {
                    String line;
                    while ((line = reader.readLine()) !=null){
                        accu.append(line);
                        accu.append("\n");
                    }
                }
                return Response.ok(String.valueOf(accu) + "\n").build();
            } else {
                return Response.notModified("production type  " + name + " not found").build();
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new WebApplicationException(e, Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    private static class IdMatcher {
        private String[] names = null;
        IdMatcher(String names) {
            if (names != null) {
                this.names = names.split(",");
            }
        }
        boolean matches(String id) {
            return names == null || Arrays.stream(names).anyMatch(x -> id.contains(x));
        }
    }
}
