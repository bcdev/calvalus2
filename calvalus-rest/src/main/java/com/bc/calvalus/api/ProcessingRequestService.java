package com.bc.calvalus.api;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.hadoop.HadoopJobHook;
import com.bc.calvalus.production.cli.CalvalusHadoopConnection;
import com.bc.calvalus.production.cli.CalvalusHadoopParameters;
import com.bc.calvalus.production.cli.CalvalusHadoopRequestConverter;
import com.bc.calvalus.production.cli.CalvalusHadoopStatusConverter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.jersey.api.NotFoundException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("processing-requests")
public class ProcessingRequestService {

    private static final SimpleDateFormat ISO_MILLIS_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    static {
        ISO_MILLIS_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
    private static String PRODUCTION_TYPE_DIR = "production-types";
    private static String PROCESSOR_DESCRIPTOR_DIR = "processor-descriptors";
    private static final TypeReference<Map<String, Object>> VALUE_TYPE_REF = new TypeReference<Map<String, Object>>() {};

    private static Logger LOG = CalvalusLogger.getLogger();

    /*
    Simple requests are those that do not require other local resources like tables, point files, shapefiles.
    post shall submit request
    get shall inquire status
    list shall list requests of user
    content shall show request content
    delete shall cancel request
    */

    /**
     * Translates a Calvalus request into a Hadoop job and submits it to RM.
     * Main parameter is the request JSON string. Secondary parameter is the service (element of URL before calvalushadooprequest).
     * Calvalus config is provided in Tomcat installation in <catalina home>/content/<service>-calvalus.properties .
     * If the request contains a productionType name the type is read from <catalina home>/etc/<type>-cht-type.json .
     * The processor descriptor <bundle path>/bundle-descriptor.xml, if available, may contain additional parameters.
     *
     * * parses request json into map
     * * creates job and sets job defaults
     * + reads type to set job parameters
     * * reads calvalus config to set job parameters
     * * reads processor descriptor to set job parameters
     * * sets job parameters from json map
     * * prepares processor deployment
     * * handles shapefile for RA, ... (?)
     * * submits job to RM
     * * returns job id
     * * handles errors by throwing WebApplicationException
     * @param requestString  Json request
     * @param request  Http request with user and url
     * @param uriInfo  not used
     * @param context  optional configuration for deputy users
     * @return  job identifier
     * @throws NotFoundException
     */
    @POST
    //@Consumes({MediaType.APPLICATION_JSON})
    public Response submit(String requestString, @Context HttpServletRequest request, @Context UriInfo uriInfo, @Context ServletContext context) throws NotFoundException {
        try {
            // determine context
            final String username = Utils.getUserName(request, context);
            final String[] userRoles = Utils.getUserRoles(request);
            final String requestUrl = request.getRequestURL().toString();
            final String serviceName = Paths.get(requestUrl).subpath(2, 3).toString();  // TODO check path
            final String catalinaHome = System.getProperty("catalina.home");
            HadoopJobHook hook = null;
            final CalvalusHadoopConnection.RoleMatcher roleMatcher = new CalvalusHadoopConnection.RoleMatcher(username, userRoles);

            final CalvalusHadoopConnection hadoopConnection = new CalvalusHadoopConnection(username);
            final CalvalusHadoopRequestConverter requestConverter = new CalvalusHadoopRequestConverter(
                    hadoopConnection,
                    username,
                    roleMatcher,
                    catalinaHome + "/content/" + PRODUCTION_TYPE_DIR,
                    catalinaHome + "/content/" + serviceName
            );

            // read Calvalus configuration
            final File calvalusConfigPath = new File(catalinaHome + "/content/" + serviceName + "/calvalus.properties");
            final Properties calvalusConfig = CalvalusHadoopRequestConverter.collectConfigParameters(calvalusConfigPath);

            // parse JSON request
            final Map<String, Object> submittedRequest = requestConverter.parseRequest(requestString);

            // merge parameters of request, configuration, production type, processor descriptor
            final CalvalusHadoopParameters hadoopParameters = requestConverter.collectParameters(
                    submittedRequest,
                    null,
                    calvalusConfig
            );

            // convert into JobConf, install processor packages, prepare output directory, and submit job
            final JobConf jobConf = requestConverter.createJob(hadoopParameters, hook);
            if (Boolean.parseBoolean(jobConf.get("overwrite", "false"))) {
                hadoopConnection.deleteOutputDir(jobConf);
            }
            final RunningJob runningJob = hadoopConnection.submitJob(jobConf);

            // return job ID
            LOG.info("Production successfully ordered with ID " + runningJob.getID());
            return Response.ok(String.valueOf(runningJob.getID()) + "\n").build();

        // pass through user exceptions
        } catch (IllegalArgumentException e) {
            LOG.log(Level.WARNING, e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("error in request: " + e.getMessage())
                    .type(MediaType.TEXT_PLAIN)
                    .build();
        // report configuration errors or temporary backend failure
        } catch (IOException e) {
            LOG.log(Level.SEVERE, e.getMessage());
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("error in configuration or request: " + e.getMessage())
                    .type(MediaType.TEXT_PLAIN)
                    .build();
        // report unexpected exceptions and configuration errors as internal server errors
        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new WebApplicationException(e,
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(e.getMessage())
                            .type(MediaType.TEXT_PLAIN)
                            .build());
        }
    }

    @DELETE
    @Path("/{name}")
    public Response cancel(@PathParam("name") String name, @Context HttpServletRequest request, @Context ServletContext context) throws NotFoundException {
        try {
            final String username = Utils.getUserName(request, context);
            final String requestUrl = request.getRequestURL().toString();
            final String serviceName = Paths.get(requestUrl).subpath(2, 3).toString();  // TODO check path
            final String catalinaHome = System.getProperty("catalina.home");

            final CalvalusHadoopConnection hadoopConnection = new CalvalusHadoopConnection(username);
            final CalvalusHadoopRequestConverter requestConverter = new CalvalusHadoopRequestConverter(hadoopConnection, username);
            final File calvalusConfigPath = new File(new File(new File(catalinaHome), "content"), serviceName + "/calvalus.properties");
            final Properties calvalusConfig = CalvalusHadoopRequestConverter.collectConfigParameters(calvalusConfigPath);
            requestConverter.collectParameters(null, null, calvalusConfig);

            final JobID jobId = JobID.forName(name);
            final RunningJob job = hadoopConnection.getJob(jobId);
            if (job != null) {
                job.killJob();
                return Response.ok(name + " cancelled\n").build();
            } else {
                return Response.notModified("job " + name + " not found for cancelling").build();
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING, e.getMessage(), e);
            throw new WebApplicationException(e, Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    private static class JobIdMatcher {
        private String[] names = null;
        JobIdMatcher(String names) {
            if (names != null) {
                this.names = names.split(",");
            }
        }
        boolean matches(String jobId) {
            return names == null || Arrays.stream(names).anyMatch(x -> jobId.equals(x) || jobId.endsWith("_" + x));
        }
    }

    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    public Response list(
            @QueryParam("names") String names,
            @Context HttpServletRequest request,
            @Context SecurityContext securityContext,
            @Context ServletContext context
    ) throws NotFoundException {
        try {
            // determine parameters and configuration
            final String username = Utils.getUserName(request, context);
            final String requestUrl = request.getRequestURL().toString();
            final String serviceName = Paths.get(requestUrl).subpath(2, 3).toString();  // TODO check path
            final String catalinaHome = System.getProperty("catalina.home");
            final JobIdMatcher jobIdMatcher = new JobIdMatcher(names);

            // retrieve jobs
            final CalvalusHadoopConnection hadoopConnection = new CalvalusHadoopConnection(username);
            final CalvalusHadoopRequestConverter requestConverter = new CalvalusHadoopRequestConverter(hadoopConnection, username);
            final File calvalusConfigPath = new File(new File(new File(catalinaHome), "content"), serviceName + "/calvalus.properties");
            final Properties calvalusConfig = CalvalusHadoopRequestConverter.collectConfigParameters(calvalusConfigPath);
            requestConverter.collectParameters(null, null, calvalusConfig);
            final JobStatus[] jobs = hadoopConnection.getAllJobs();

            // collect status entries
            StringBuilder accu = new StringBuilder();
            CalvalusHadoopStatusConverter statusConverter = CalvalusHadoopStatusConverter.create(hadoopConnection, "json");
            statusConverter.initialiseJobStatus(accu);
            for (JobStatus job : jobs) {
                if (username.equals(job.getUsername()) && jobIdMatcher.matches(job.getJobId())) {
                    statusConverter.accumulateJobStatus(job.getJobId(), job, accu);
                }
            }
            statusConverter.finaliseJobStatus(accu);

            return Response.ok(String.valueOf(accu) + "\n").build();
        } catch (ServletException | IOException | InterruptedException | IllegalAccessException |
                 InvocationTargetException | NoSuchMethodException e) {
            LOG.log(Level.WARNING, e.getMessage(), e);
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
    public Response show(@PathParam("name") String name, @Context HttpServletRequest request, @Context SecurityContext securityContext, @Context ServletContext context) throws NotFoundException {
        try {
            final String username = Utils.getUserName(request, context);
            final String requestUrl = request.getRequestURL().toString();
            final String serviceName = Paths.get(requestUrl).subpath(2, 3).toString();  // TODO check path
            final String catalinaHome = System.getProperty("catalina.home");

            final CalvalusHadoopConnection hadoopConnection = new CalvalusHadoopConnection(username);
            final CalvalusHadoopRequestConverter requestConverter = new CalvalusHadoopRequestConverter(hadoopConnection, username);
            final File calvalusConfigPath = new File(new File(new File(catalinaHome), "content"), serviceName + "/calvalus.properties");
            final Properties calvalusConfig = CalvalusHadoopRequestConverter.collectConfigParameters(calvalusConfigPath);
            requestConverter.collectParameters(null, null, calvalusConfig);

            JobID jobId = JobID.forName(name);
            RunningJob job = hadoopConnection.getJob(jobId);
            if (job != null) {
                StringBuilder accu = new StringBuilder();
                CalvalusHadoopStatusConverter statusConverter = CalvalusHadoopStatusConverter.create(hadoopConnection, "json");
                statusConverter.initialiseJobStatus(accu);
                statusConverter.accumulateJob(name, job, accu);
                statusConverter.finaliseJobStatus(accu);
                return Response.ok(String.valueOf(accu) + "\n").build();
            } else {
                return Response.notModified("job " + name + " not found").build();
            }
        } catch (WebApplicationException e) {
            throw e;
        } catch (Exception e) {
            throw new WebApplicationException(e, Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }
}
