package com.bc.calvalus.ogcapiprocesses;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.ogcapiprocesses.model.OGCJobList;
import com.bc.calvalus.ogcapiprocesses.model.OGCJobStatus;
import com.bc.calvalus.ogcapiprocesses.model.OGCLink;
import com.bc.calvalus.ogcapiprocesses.model.OGCStatusInfo;
import com.bc.calvalus.production.cli.CalvalusHadoopConnection;
import com.bc.calvalus.production.cli.CalvalusHadoopParameters;
import com.bc.calvalus.production.cli.CalvalusHadoopRequestConverter;
import com.bc.calvalus.production.util.DescriptorUtils;
import com.bc.calvalus.upload.Utils;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobID;

import javax.print.attribute.standard.Media;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@Path("jobs")
public class JobsService {
    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final CalvalusToOGCProcessTranslator CALVALUS_TO_OGC_PROCESS_TRANSLATOR = new CalvalusToOGCProcessTranslator();
    private static final String PRODUCTION_TYPE_DIR = "production-types";

    @GET
    //@Produces({MediaType.APPLICATION_JSON})
    public Response list(
        @Context HttpServletRequest request,
        @Context SecurityContext securityContext,
        @Context ServletContext context
    ) {
        try {
            final String requestUrl = request.getRequestURL().toString();
            final String username = Utils.getUserName(request, context);
            LOG.info("user: " + username);
            final String catalinaHome = System.getProperty("catalina.home");
            final String serviceName = Paths.get(requestUrl).subpath(2, 3).toString();  // TODO check path
            final File calvalusConfigPath = new File(catalinaHome + "/content/" + serviceName + "/calvalus.properties");
            final Properties calvalusConfig = CalvalusHadoopRequestConverter.collectConfigParameters(calvalusConfigPath);
            // final String[] userRoles = Utils.getUserRoles(request);
            // final String catalinaHome = System.getProperty("catalina.home");
            final CalvalusHadoopConnection hadoopConnection = new CalvalusHadoopConnection(username);
            final CalvalusHadoopRequestConverter requestConverter = new CalvalusHadoopRequestConverter(
                    hadoopConnection,
                    username,
                    null,
                    catalinaHome + "/content/" + PRODUCTION_TYPE_DIR,
                    null
            );
            final CalvalusHadoopParameters hadoopParameters = requestConverter.collectParameters(
                    null,
                    null,
                    calvalusConfig
            );
            hadoopConnection.createJobClient(hadoopParameters);
            //
            List<JobStatus> hadoopJobStatuses = Arrays.stream(hadoopConnection.getAllJobs()).filter(status -> username.equals(status.getUsername())).collect(Collectors.toList());
            ArrayList<OGCStatusInfo> jobs = new ArrayList<>();
            ArrayList<OGCLink> links = new ArrayList<>();
            OGCJobList jobList = new OGCJobList();
            jobList.jobs = jobs;
            jobList.links = links;

            for (JobStatus jobStatus : hadoopJobStatuses) {
                String jobID = jobStatus.getJobID().toString();
                OGCJobStatus ogcJobStatus = CALVALUS_TO_OGC_PROCESS_TRANSLATOR.getOgcJobStatus(jobStatus);
                if (ogcJobStatus == null) {
                    throw new IllegalArgumentException("OGC Job status for jobID '" + jobID + "' is null, hadoop Job status: " + jobStatus + " with run state: " + jobStatus.getRunState());
                }
                OGCStatusInfo statusInfo = new OGCStatusInfo();
                statusInfo.status = ogcJobStatus;
                statusInfo.jobID = jobID;
                jobs.add(statusInfo);

                OGCLink link  = new OGCLink();
                link.href = requestUrl + "/" + jobID;
                link.rel = "status";
                link.type = MediaType.APPLICATION_JSON;
                link.title = "Status of job " + jobID;
                links.add(link);
            }

            OGCLink self = OGCLink.self(requestUrl);
            links.add(self);

            return Response.ok(jobList).type(MediaType.APPLICATION_JSON).build();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .type(MediaType.TEXT_PLAIN)
                    .entity(e.getMessage())
                    .build();
        }
    }

    @GET
    @Path("/{jobID}")
    public Response show(
            @Context HttpServletRequest request,
            @Context ServletContext context,
            @PathParam("jobID") String jobID
            //OGCStatusInfo requestContent
    ) {
        try {
            final String requestUrl = request.getRequestURL().toString();
            final String username = Utils.getUserName(request, context);
            // final String[] userRoles = Utils.getUserRoles(request);
            final String catalinaHome = System.getProperty("catalina.home");
            final CalvalusHadoopConnection hadoopConnection = new CalvalusHadoopConnection(username);
            final String serviceName = Paths.get(requestUrl).subpath(2, 3).toString();  // TODO check path
            final File calvalusConfigPath = new File(catalinaHome + "/content/" + serviceName + "/calvalus.properties");
            final Properties calvalusConfig = CalvalusHadoopRequestConverter.collectConfigParameters(calvalusConfigPath);
            LOG.info("Loaded calvalus config");
            final CalvalusHadoopRequestConverter requestConverter = new CalvalusHadoopRequestConverter(
                    hadoopConnection,
                    username,
                    null,
                    catalinaHome + "/content/" + PRODUCTION_TYPE_DIR,
                    null
            );
            final CalvalusHadoopParameters hadoopParameters = requestConverter.collectParameters(
                    null,
                    null,
                    calvalusConfig
            );
            hadoopConnection.createJobClient(hadoopParameters);
            JobStatus hadoopJobStatus = hadoopConnection.getJobStatus(JobID.forName(jobID));
            OGCJobStatus OgcJobStatus = CalvalusToOGCProcessTranslator.getOgcJobStatus(hadoopJobStatus);


            OGCStatusInfo statusInfo = new OGCStatusInfo();
            // TODO check access rights
            statusInfo.jobID = jobID;
            statusInfo.status = OgcJobStatus;
            statusInfo.type = "process";
            // statusInfo.processID

            return Response.ok(statusInfo).type(MediaType.APPLICATION_JSON).build();
        } catch (Exception e) {
            LOG.severe(e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .type(MediaType.TEXT_PLAIN)
                    .entity(e.getMessage())
                    .build();
        }
    }
}