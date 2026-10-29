package com.bc.calvalus.ogcapiprocesses;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.ogcapiprocesses.model.*;
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
import java.util.Optional;
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

    private CalvalusProcessorRepository repository;
    private final CalvalusToOGCProcessTranslator translator;

    public ProcessDescriptionService(CalvalusProcessorRepository repository, CalvalusToOGCProcessTranslator translator) {
        this.translator = translator;
    }

    public ProcessDescriptionService() {
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
            final String catalinaHome = System.getProperty("catalina.home");
            final String serviceName = getServiceName(context);
            final String serviceDir = catalinaHome + "/content/" + serviceName;
            final String username = Utils.getUserName(request, context);
            final String[] userRoles = Utils.getUserRoles(request);
            final String maxAge = request.getHeader("max-age");
            final DescriptorUtils.RoleMatcher roleMatcher = new DescriptorUtils.RoleMatcher(username, userRoles);
            final Response notFoundResponse = Response
                    .status(ClientResponse.Status.NOT_FOUND)
                    .entity("Could not retrieve process '" + processID + "'").build();

            // TODO repository should be created per server, not per request
            // TODO populate registry
            final CalvalusProcessorRegistry registry = new CalvalusProcessorRegistry();
            this.repository = new CalvalusProcessorRepository(serviceDir, registry);

            ObjectMapper mapper = new ObjectMapper();

            String processDescriptorContent = null;
            boolean success = true;
            final CalvalusHadoopConnection hadoopConnection = new CalvalusHadoopConnection(username);
            final CalvalusHadoopParameters hadoopParameters = readCalvalusConfiguration(getCalvalusConfigPath(catalinaHome, serviceName));
            hadoopConnection.createJobClient(hadoopParameters);

            Optional<File> localDescritorPath = this.repository.localDescriptorPath(processID);
            if (!localDescritorPath.isPresent()) {
                return notFoundResponse;
            }
            if ("0".equals(maxAge) || ! localDescritorPath.get().exists()) {
                success = this.repository.refreshCache(hadoopConnection, processID, username);
                success = success || this.repository.refreshCache(hadoopConnection, processID, null);
            }

            boolean has_access = this.repository.checkAccess(processID, roleMatcher);
            if (!success || !has_access) {
                return notFoundResponse;
            }

            Optional<String> content = this.repository.descriptorContent(processID);
            if (!content.isPresent()) {
                return notFoundResponse;
            } else {
                processDescriptorContent = content.get();
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
