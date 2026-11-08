package com.bc.calvalus.ogcapiprocesses;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.ogcapiprocesses.model.OGCProcessDescription;
import com.bc.calvalus.ogcapiprocesses.model.CalvalusFullProcessorDescriptor;
import com.bc.calvalus.production.util.DescriptorUtils;
import com.bc.calvalus.upload.Utils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.jersey.api.NotFoundException;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("processes")
public class ProcessesService {

    private static Logger LOG = CalvalusLogger.getLogger();
    private static final String PACKAGE_SEPARATOR = "~";
    private static final CalvalusToOGCProcessTranslator CALVALUS_TO_OGC_PROCESS_TRANSLATOR = new CalvalusToOGCProcessTranslator();

    @OPTIONS
    public Response options() {
        LOG.info("options called for /processes");
        return Response.ok("")
                .header("Access-Control-Allow-Origin", "http://127.0.0.1:33495")
                .header("Access-Control-Allow-Headers", "origin, content-type, accept, authorization")
                .header("Access-Control-Allow-Credentials", "true")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                .header("Access-Control-Max-Age", "1209600")
                .build();
    }

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
            //final DescriptorUtils.IdMatcher idMatcher = new DescriptorUtils.IdMatcher(null);
            final DescriptorUtils.RoleMatcher roleMatcher = new DescriptorUtils.RoleMatcher(username, userRoles);

            ObjectMapper objectMapper = new ObjectMapper();
            ObjectNode accu = objectMapper.createObjectNode();
            ArrayNode processes = accu.putArray("processes");
            
            // find packages and descriptor files in packages
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
                            
                            // check authorisation to see this processor
                            final String processorName = filename.substring(
                                    0,
                                    filename.length() - "-descriptor.json".length()
                            );
                            if (//idMatcher.matches(processorPackageDir.getName() + "/" + processorName) &&
                                    roleMatcher.matches(processorPackageDir, filename)) {
                                
                                // create process JSON structure
                                final String packageName = processorPackageDir.getName();
                                final String version = packageName.substring(packageName.lastIndexOf('-')+1);
                                addProcessToOgcApiProcesses(
                                        packageName + PACKAGE_SEPARATOR + processorName, version, requestUrl, processes
                                );
                            }
                        }
                    }
                }
            }
            ArrayNode links = accu.putArray("links");
            ObjectNode linksObject = links.addObject();
            linksObject.put("href", requestUrl);
            linksObject.put("rel", "self");
            linksObject.put("type", "application/json");
            return Response.ok(objectMapper.writeValueAsString(accu))
                .header("Access-Control-Allow-Origin", "http://127.0.0.1:33495")
                .header("Access-Control-Allow-Headers", "origin, content-type, accept, authorization")
                .header("Access-Control-Allow-Credentials", "true")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                .header("Access-Control-Max-Age", "1209600")
                    .build();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new WebApplicationException(e,
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(e.getMessage())
                            .type(MediaType.TEXT_PLAIN)
                .header("Access-Control-Allow-Origin", "http://127.0.0.1:33495")
                .header("Access-Control-Allow-Headers", "origin, content-type, accept, authorization")
                .header("Access-Control-Allow-Credentials", "true")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                .header("Access-Control-Max-Age", "1209600")
                            .build());
        }
    }

    private static void addProcessToOgcApiProcesses(
            String processorName,
            String version,
            String requestUrl,
            ArrayNode processes
    ) {
        ObjectNode processObject = processes.addObject();
        processObject.put("id", processorName);
        processObject.put("title", processorName);
        processObject.put("version", version);
        ArrayNode jobControlOptions = processObject.putArray("jobControlOptions");
        jobControlOptions.add("async-execute");
        ArrayNode outputTransmission = processObject.putArray("outputTransmission");
        outputTransmission.add("reference");
        ArrayNode links = processObject.putArray("links");
        ObjectNode linksObject = links.addObject();
        linksObject.put("href", requestUrl + "/" + processorName);
        linksObject.put("type", "application/json");
        linksObject.put("rel", "self");
        linksObject.put("title", "process description");
    }

    @OPTIONS
    @Path("/{packageTildeName}")
    public Response options2(
            @PathParam("packageTildeName") String pkgAndName) {
        LOG.info("options called for /processes/" + pkgAndName);
        return Response.ok("")
                .header("Access-Control-Allow-Origin", "http://127.0.0.1:33495")
                .header("Access-Control-Allow-Headers", "origin, content-type, accept, authorization")
                .header("Access-Control-Allow-Credentials", "true")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                .header("Access-Control-Max-Age", "1209600")
                .build();
    }
    @GET
    @Path("/{packageTildeName}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response show(
            @PathParam("packageTildeName") String pkgAndName,
            @Context HttpServletRequest request,
            @Context SecurityContext securityContext,
            @Context ServletContext context
    ) throws NotFoundException {
        final int pos = pkgAndName.indexOf(PACKAGE_SEPARATOR);
        final String pkg = pkgAndName.substring(0, pos);
        final String name = pkgAndName.substring(pos + 1);

        try {
            final String username = Utils.getUserName(request, context);
            final String[] userRoles = Utils.getUserRoles(request);
            final String requestUrl = request.getRequestURL().toString();
            final String serviceName = Paths.get(requestUrl).subpath(2, 3).toString();  // TODO check path
            final String catalinaHome = System.getProperty("catalina.home");
            final String serviceDir = catalinaHome + "/content/" + serviceName;
            final String localPath = serviceDir + "/" + pkg + "/" + name + "-descriptor.json";
            //final DescriptorUtils.IdMatcher idMatcher = new DescriptorUtils.IdMatcher(names);
            final DescriptorUtils.RoleMatcher roleMatcher = new DescriptorUtils.RoleMatcher(username, userRoles);

            // read from cache
            if (new File(localPath).exists()) {
                ObjectMapper jsonParser = new ObjectMapper();
                jsonParser.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
                jsonParser.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
                String descriptorContent = readFileContent(localPath);
                if (roleMatcher.matches(descriptorContent)) {
                    CalvalusFullProcessorDescriptor calvalusFullDescriptor =
                            jsonParser.readValue(descriptorContent, CalvalusFullProcessorDescriptor.class);
                    OGCProcessDescription ogcProcessDescription =
                            CALVALUS_TO_OGC_PROCESS_TRANSLATOR.toOgcProcessDescription(
                                    calvalusFullDescriptor.processorDescriptor, pkgAndName);
                    return Response.ok(ogcProcessDescription)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "origin, content-type, accept, authorization")
                .header("Access-Control-Allow-Credentials", "true")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                .header("Access-Control-Max-Age", "1209600").build();
                }
            }
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity("processor descriptor  " + pkg + "/" + name + " not found in cache")
                .header("Access-Control-Allow-Origin", "http://127.0.0.1:33495")
                .header("Access-Control-Allow-Headers", "origin, content-type, accept, authorization")
                .header("Access-Control-Allow-Credentials", "true")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                .header("Access-Control-Max-Age", "1209600")
                    .build();

        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new WebApplicationException(e,
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(e.getMessage())
                            .type(MediaType.TEXT_PLAIN)
                .header("Access-Control-Allow-Origin", "http://127.0.0.1:33495")
                .header("Access-Control-Allow-Headers", "origin, content-type, accept, authorization")
                .header("Access-Control-Allow-Credentials", "true")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                .header("Access-Control-Max-Age", "1209600")
                            .build());
        }
    }

    private static String readFileContent(String localPath) throws IOException {
        final StringBuilder accu = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(localPath))) {
            String line;
            while ((line = reader.readLine()) !=null){
                accu.append(line);
                accu.append("\n");
            }
        }
        return String.valueOf(accu);
    }
}