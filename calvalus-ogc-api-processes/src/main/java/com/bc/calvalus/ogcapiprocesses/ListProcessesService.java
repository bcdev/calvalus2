package com.bc.calvalus.ogcapiprocesses;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.cli.CalvalusHadoopParameters;
import com.bc.calvalus.production.cli.CalvalusHadoopRequestConverter;
import com.bc.calvalus.production.util.DescriptorUtils;
import com.sun.jersey.api.NotFoundException;
import com.bc.calvalus.production.cli.CalvalusHadoopConnection;
import com.sun.jersey.api.client.ClientResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("processes")
public class ListProcessesService {

    private static Logger LOG = CalvalusLogger.getLogger();

    @GET
    @Produces({MediaType.APPLICATION_JSON})
    public Response list(
            @Context HttpServletRequest request,
            @Context SecurityContext securityContext,
            @Context ServletContext context
    ) {
        try {
            final String username = Utils.getUserName(request, context);
            final String[] userRoles = Utils.getUserRoles(request);
            final String requestUrl = request.getRequestURL().toString();
            final String serviceName = Paths.get(requestUrl).subpath(2, 3).toString();  // TODO check path
            final String catalinaHome = System.getProperty("catalina.home");
            final String serviceDir = catalinaHome + "/content/" + serviceName;
            final String maxAge = request.getHeader("max-age");
            final DescriptorUtils.IdMatcher idMatcher = new DescriptorUtils.IdMatcher(null);
            final DescriptorUtils.RoleMatcher roleMatcher = new DescriptorUtils.RoleMatcher(username, userRoles);

            /*
            if ("0".equals(maxAge)) {
                final File calvalusConfigPath = new File(catalinaHome + "/content/" + serviceName + "/calvalus.properties");
                final CalvalusHadoopParameters hadoopParameters = readCalvalusConfiguration(calvalusConfigPath);
                final CalvalusHadoopConnection hadoopConnection = new CalvalusHadoopConnection(username);
                hadoopConnection.createJobClient(hadoopParameters);
                final StringBuilder accu = new StringBuilder("[");
                stageProcessorDescriptors(hadoopConnection, username, serviceDir, idMatcher, roleMatcher, accu);
                stageProcessorDescriptors(hadoopConnection, null, serviceDir, idMatcher, roleMatcher, accu);
                accu.append("]");
                return Response.ok(String.valueOf(accu) + "\n").build();
            }
            */

            ObjectMapper objectMapper = new ObjectMapper();
            ObjectNode accu = objectMapper.createObjectNode();
            ArrayNode processes = accu.putArray("processes");
            final File[] processorPackageDirs = new File(serviceDir).listFiles(
                    (File file, String name) -> new File(file, name).isDirectory()
            );
            if (processorPackageDirs != null) {
                for (File processorPackageDir : processorPackageDirs) {
                    final String[] processorDescriptorFilenames = processorPackageDir.list(
                            (File _file, String name) -> name.endsWith("-descriptor.json")
                    );
                    if (processorDescriptorFilenames != null) {
                        for (String filename : processorDescriptorFilenames) {
                            final String processorName = filename.substring(
                                    0,
                                    filename.length() - "-descriptor.json".length()
                            );

                            if (idMatcher.matches(processorPackageDir.getName() + "/" + processorName) &&
                                    roleMatcher.matches(processorPackageDir, filename)) {
                                ObjectNode processObject = processes.addObject();
                                processObject.put("id", processorName);
                                processObject.put("title", processorName);
                                processObject.put("version", processorPackageDir.getName());
                                ArrayNode jobControlOptions = processObject.putArray("jobControlOptions");
                                jobControlOptions.add("async-execute");
                                ArrayNode outputTransmission = processObject.putArray("outputTransmission");
                                outputTransmission.add("reference");
                                ArrayNode links = processObject.putArray("links");
                                ObjectNode linksObject = links.addObject();
                                // TODO hardcoded URL
                                linksObject.put("href", "http://master00:8080/bc-ogc-api-processes/processes/" + processorName);
                                linksObject.put("type", "application/json");
                                linksObject.put("rel", "self");
                                linksObject.put("title", "process description");
                            }
                        }
                    }
                }
            }
            ArrayNode links = accu.putArray("links");
            return Response.ok(objectMapper.writeValueAsString(accu)).build();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new WebApplicationException(e,
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(e.getMessage())
                            .type(MediaType.TEXT_PLAIN)
                            .build());
        }
    }
}