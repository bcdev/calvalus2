package com.bc.calvalus.ogcapiprocesses;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.ogcapiprocesses.model.CalvalusFullProcessorDescriptor;
import com.bc.calvalus.ogcapiprocesses.model.CalvalusProcessorDescriptor;
import com.bc.calvalus.ogcapiprocesses.model.OGCProcessDescription;
import com.bc.calvalus.ogcapiprocesses.model.ProcessDescriptorOld;
import com.bc.calvalus.production.cli.CalvalusHadoopParameters;
import com.bc.calvalus.production.cli.CalvalusHadoopRequestConverter;
import com.bc.calvalus.production.util.DescriptorUtils;
import com.fasterxml.jackson.databind.JsonNode;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.*;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Process Description Endpoint: GET /processes/{processID}
 * OGC API Processes core
 *
 * /req/core/process
 * https://docs.ogc.org/is/18-062r2/18-062r2.html#toc31
 */
@Path("processes/{processID}")
@Produces({ MediaType.APPLICATION_JSON })
public class ProcessDescriptionService {
    private static final String CALVALUS_PROPERTIES_FILENAME = "calvalus.properties";
    private static final Logger LOG = CalvalusLogger.getLogger();

    private CalvalusProcessRepository repository;
    private CalvalusToOGCProcessTranslator translator;

    public ProcessDescriptionService(CalvalusProcessRepository repository, CalvalusToOGCProcessTranslator translator) {
        this.repository = repository;
        this.translator = translator;
    }

    public ProcessDescriptionService() {
        this.repository = new CalvalusProcessRepository();
        this.translator = new CalvalusToOGCProcessTranslator();
    }

    @GET
    public Response show(
            @PathParam("processID") String processID,
            @Context HttpServletRequest request,
            @Context SecurityContext securityContext,
            @Context ServletContext context,
            @Context UriInfo uriInfo
            ) throws NotFoundException {

        try {
            // TODO old
            final String username = Utils.getUserName(request, context);
            final String[] userRoles = Utils.getUserRoles(request);
            final String maxAge = request.getHeader("max-age");
            final String serviceName = getServiceName(context);
            final String catalinaHome = System.getProperty("catalina.home");
            final DescriptorUtils.RoleMatcher roleMatcher = new DescriptorUtils.RoleMatcher(username, userRoles);
            final String serviceDir = catalinaHome + "/content/" + serviceName;
            // TODO old end

            ObjectMapper mapper = new ObjectMapper();
            // TODO hardcoded example
            final String processorName = "c2rcc-14.0";
            final String content = "{}";

            ProcessDescriptorOld processDescriptor = new ProcessDescriptorOld(
                    processorName,
                    processID,
                    serviceDir
            );

            String processDescriptorContent = null;
            boolean success = true;
            final CalvalusHadoopConnection hadoopConnection = new CalvalusHadoopConnection(username);
            final CalvalusHadoopParameters hadoopParameters = readCalvalusConfiguration(getCalvalusConfigPath(catalinaHome, serviceName));
            hadoopConnection.createJobClient(hadoopParameters);

            if ("0".equals(maxAge) || ! processDescriptor.localPath().exists()) {
                success = refreshCache(
                        processDescriptor,
                        hadoopConnection
                );
            }
            if (success) {
                processDescriptorContent = processDescriptor.getContent(hadoopConnection, username);
            }
            if (!success || !roleMatcher.matches(processDescriptorContent)) {
                return Response
                        .status( ClientResponse.Status.NOT_FOUND)
                        .entity("Could not retrieve " + processDescriptor + "/"  + processorName).build();
            }

            if (processDescriptorContent == null) {
                return Response.status( ClientResponse.Status.NOT_FOUND).build();
            }
            CalvalusFullProcessorDescriptor calvalusFullDescriptor = mapper.readValue(processDescriptorContent, CalvalusFullProcessorDescriptor.class);
            CalvalusProcessorDescriptor calvalusDescriptor = calvalusFullDescriptor.processorDescriptor;
            OGCProcessDescription ogcProcessDescription = this.translator.toOgcProcessDescription(calvalusDescriptor, processID);
            return Response.ok(ogcProcessDescription).build();

        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new WebApplicationException(e,
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(e.getMessage())
                            .type(MediaType.TEXT_PLAIN)
                            .build());
        }
    }

    /**
     * Retrieve process descriptor file and store locally
     * @param processDescriptor the process descriptor to fetch
     * @param hadoopConnection CalvalusHadoopConnection to access the reference files
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    private boolean refreshCache(
        ProcessDescriptorOld processDescriptor,
        CalvalusHadoopConnection hadoopConnection
    ) throws  IOException, InterruptedException {
        String descriptorContent;
        try (PrintStream out = new PrintStream(processDescriptor.localPath())) {
            // TODO allow user-local processes
            descriptorContent = processDescriptor.getContent(hadoopConnection, null);
            out.print(descriptorContent);
        }

        return descriptorContent == null;
    }

    // TODO add a class (OGC process description) for this object that with an instance to be constructed from a processor descriptor
    /**
     * Builds an OGC process description according to https://docs.ogc.org/is/18-062r2/18-062r2.html#ogc_process_description.
     * @param processDescriptor
     * @param processDescriptorContent
     * @return
     */
    private String buildSuccessResponseContent(ProcessDescriptorOld processDescriptor, JsonNode processDescriptorContent) {
        ObjectMapper reponseMapper = new ObjectMapper();
        ObjectNode accu = reponseMapper.createObjectNode();
        accu.put("id", processDescriptor.getProcessID());
        accu.put("title", processDescriptorContent.get("title").asText());
        accu.put("description", processDescriptorContent.get("description").asText());
        ArrayNode outputTransmission = accu.putArray("outputTransmission");
        outputTransmission.add("reference");
        ArrayNode inputs = accu.putArray("inputs");
        // TODO continue

        return accu.toString();
    }


    private File getCalvalusConfigPath(String catalinaHome, String serviceName) {
        return new File(catalinaHome + "/content/" + serviceName + "/" + CALVALUS_PROPERTIES_FILENAME);
    }

    private String getServiceName(ServletContext servletContext) {
        return servletContext.getContextPath().replaceFirst("^/", "");
    }

    private static CalvalusHadoopParameters readCalvalusConfiguration(File calvalusConfigPath) throws IOException {
        final Properties calvalusConfig = CalvalusHadoopRequestConverter.collectConfigParameters(calvalusConfigPath);
        CalvalusHadoopParameters hadoopParameters = new CalvalusHadoopParameters();
        CalvalusHadoopRequestConverter.setHadoopDefaultParameters(hadoopParameters);
        for (Map.Entry<Object, Object> entry : calvalusConfig.entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (key.startsWith("calvalus.hadoop.")) {
                key = key.substring("calvalus.hadoop.".length());
            }
            hadoopParameters.set(key, String.valueOf(entry.getValue()));
        }
        return hadoopParameters;
    }
}
