package com.bc.calvalus.api;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.cli.CalvalusHadoopConnection;
import com.sun.jersey.api.NotFoundException;

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
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("example-requests")
public class ExampleRequestService {

    private static Logger LOG = CalvalusLogger.getLogger();

    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    public Response list(
            @QueryParam("names") String names,
            @Context HttpServletRequest request,
            @Context SecurityContext securityContext,
            @Context ServletContext context
    ) throws NotFoundException {
        try {
            final String username = Utils.getUserName(request, context);
            final String requestUrl = request.getRequestURL().toString();
            final String serviceName = Paths.get(requestUrl).subpath(2, 3).toString();  // TODO check path
            final String catalinaHome = System.getProperty("catalina.home");
            final String serviceDir = catalinaHome + "/content/" + serviceName;
            final CalvalusHadoopConnection.IdMatcher idMatcher = new CalvalusHadoopConnection.IdMatcher(names);

            final File[] processorPackageDirs = new File(serviceDir).listFiles((File file, String name) -> new File(file, name).isDirectory());
            StringBuilder accu = new StringBuilder("[");
            if (processorPackageDirs != null) {
                for (File processorPackageDir : processorPackageDirs) {
                    final String[] processorDescriptorFilenames = processorPackageDir.list((File _file, String name) -> name.endsWith("-example-request.json"));
                    if (processorDescriptorFilenames != null) {
                        for (String filename : processorDescriptorFilenames) {
                            final String id = filename.substring(0, filename.length() - "-example-request.json".length());
                            if (idMatcher.matches(processorPackageDir.getName() + "/" + id)) {
                                if (accu.length() > 1) {
                                    accu.append(", ");
                                }
                                accu.append("\"");
                                accu.append(processorPackageDir.getName());
                                accu.append("/");
                                accu.append(id);
                                accu.append("\"");
                            }
                        }
                    }
                }
            }
            accu.append("]");

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
    @Path("/{package}/{name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    public Response show(@PathParam("package") String pkg, @PathParam("name") String name, @Context HttpServletRequest request, @Context SecurityContext securityContext, @Context ServletContext context) throws NotFoundException {
        try {
            final String username = Utils.getUserName(request, context);
            final String requestUrl = request.getRequestURL().toString();
            final String serviceName = Paths.get(requestUrl).subpath(2, 3).toString();  // TODO check path
            final String catalinaHome = System.getProperty("catalina.home");
            final String serviceDir = catalinaHome + "/content/" + serviceName;
            final String localPath = serviceDir + "/" + pkg + "/" + name + "-example-request.json";

            if (new File(localPath).exists()) {
                final StringBuilder accu = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(new FileReader(localPath))) {
                    String line;
                    while ((line = reader.readLine()) !=null){
                        accu.append(line);
                        accu.append("\n");
                    }
                }
                return Response.ok(String.valueOf(accu)).build();
            }
            return Response.notModified("processor descriptor  " + pkg + "/" + name + " not found").build();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new WebApplicationException(e, Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }
}
