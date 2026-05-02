package com.bc.calvalus.api;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.hadoop.HadoopJobHook;
import com.bc.calvalus.production.cli.CalvalusHadoopConnection;
import com.bc.calvalus.production.cli.CalvalusHadoopParameters;
import com.bc.calvalus.production.cli.CalvalusHadoopRequestConverter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.jersey.api.NotFoundException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("calvalushadooprequests")
public class CalvalusHadoopWebService {

    private static final SimpleDateFormat ISO_MILLIS_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    static {
        ISO_MILLIS_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
    private static final TypeReference<Map<String, Object>> VALUE_TYPE_REF = new TypeReference<Map<String, Object>>() {};

    private static Logger LOG = CalvalusLogger.getLogger();

    /*
    get soll Status erfragen und zurückgeben
    list soll Requests des Nutzers zurückgeben, ggf. begrenzt auf die letzten n
    content soll die Parameter des Request erfragen und zurückgeben (oder wir speichern den Request)
    delete soll den Request canceln
    put - kann Hadoop einen Parameter ändern?
    */

    /**
     * Translates a Calvalus request into a Hadoop job and submits it to RM.
     * Main parameter is the request JSON string.
     * Calvalus config is provided in Tomcat installation in <catalina home>/content/<service>-calvalus.properties .
     * The request contains a productionType name. The type is read from <catalina home>/etc/<type>-cht-type.json .
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
            final String user = Utils.getUserName(request, context);
            final String requestUrl = request.getRequestURL().toString();
            final String serviceName = Paths.get(requestUrl).subpath(2, 3).toString();  // TODO check path
            final String catalinaHome = System.getProperty("catalina.home");
            HadoopJobHook hook = null;

            // check request
            System.out.println(user + " " + requestString);
            if (requestString == null) {
                throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
                                                          .entity("missing request content")
                                                          .type(MediaType.TEXT_PLAIN)
                                                          .build());
            }

            // convert submitted request into Hadoop job
            final CalvalusHadoopConnection hadoopConnection = new CalvalusHadoopConnection(user);
            final CalvalusHadoopRequestConverter requestConverter = new CalvalusHadoopRequestConverter(hadoopConnection, user);
            final File calvalusConfigPath = new File(new File(new File(catalinaHome), "content"), serviceName + "-calvalus.properties");
            final Properties calvalusConfig = CalvalusHadoopRequestConverter.collectConfigParameters(calvalusConfigPath);
            final Map<String, Object> submittedRequest = requestConverter.parseRequest(requestString);
            final CalvalusHadoopParameters hadoopParameters = requestConverter.collectParameters(submittedRequest, null, calvalusConfig);
            final JobConf jobConf = requestConverter.createJob(hadoopParameters, hook);

            // prepare output directory and submit job
            if (Boolean.parseBoolean(jobConf.get("overwrite", "false"))) {
                hadoopConnection.deleteOutputDir(jobConf);
            }
            final RunningJob runningJob = hadoopConnection.submitJob(jobConf);
            LOG.info("Production successfully ordered with ID " + runningJob.getID());

            // return job ID
            return Response.ok(String.valueOf(runningJob.getID()) + "\n").build();
        } catch (WebApplicationException e) {
            throw e;
        } catch (Exception e) {
            throw new WebApplicationException(e,
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(e.getMessage())
                            .type(MediaType.TEXT_PLAIN)
                            .build());
        }
    }

    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    public Response list(@Context HttpServletRequest request, @Context SecurityContext securityContext, @Context ServletContext context) throws NotFoundException {
        Response response;
        try {
            final String user = Utils.getUserName(request, context);
            final List<CalvalusHadoopRequestEntry> requests = new ArrayList<CalvalusHadoopRequestEntry>();
            // test current working directory
            final String launchDir = new File(System.getProperty("user.dir")).getAbsolutePath().toString();
            // test access to the URL of this service
            final String requestUrl = request.getRequestURL().toString();
            // test finding tomcat installation directory
            final String catalinaHome = System.getProperty("catalina.home");
            // test reading content from absolute path
            final String processingTypeFile = "/home/martin/projects/calvalus-instances/share/etc/processing-cht-type.json";
            final String processingTypeContent = new String(Files.readAllBytes(Paths.get(processingTypeFile)), StandardCharsets.UTF_8);
            requests.add(new CalvalusHadoopRequestEntry(catalinaHome, launchDir + " " + requestUrl + " " + processingTypeContent, new Properties()));
            final int count = requests.size();
            response = Response.ok(requests).header("X-Total-Count", count).build();
        } catch (ServletException | IOException e) {
            LOG.log(Level.WARNING, e.getMessage(), e);
            throw new WebApplicationException(e,
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(e.getMessage())
                            .type(MediaType.TEXT_PLAIN)
                            .build());
        }
        return response;
    }

    /*
    @GET
    @Path("/{id}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    public CalvalusHadoopRequestEntry show(@PathParam("id") String id, @Context HttpServletRequest request, @Context SecurityContext securityContext, @Context ServletContext context) throws NotFoundException {
        try {
            String userName = Utils.getUserName(request, context);
            CalvalusHadoopRequestModel model = CalvalusHadoopRequestModel.getInstance(context);
            CalvalusHadoopRequestEntry calvalusHadoopRequest = model.findRequest(userName, id);
            if (calvalusHadoopRequest == null) {
                throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
            }
            return calvalusHadoopRequest;
        } catch (WebApplicationException e) {
            throw e;
        } catch (Exception e) {
            throw new WebApplicationException(e, Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    @POST
    @Consumes({MediaType.MULTIPART_FORM_DATA})
    public Response submit(@Context HttpServletRequest request, @Context UriInfo uriInfo, @Context ServletContext context) throws NotFoundException {
        try {
            CalvalusHadoopRequestModel model = CalvalusHadoopRequestModel.getInstance(context);
            final String user = Utils.getUserName(request, context);
            final FileItem item = getFileItem(request);
            if (item == null) {
                throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
                                                          .entity("missing zip file content")
                                                          .type(MediaType.TEXT_PLAIN)
                                                          .build());
            }
            String zipFileName = item.getName();
            if (!zipFileName.endsWith(".zip")) {
                throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
                                                          .entity("shapefile upload requires flat zip")
                                                          .type(MediaType.TEXT_PLAIN)
                                                          .build());
            }
            String shapeName = shapefileModel.getName(zipFileName);
            String shapefilePath = shapefileModel.getUserPath(userName, ShapefileModel.REGION_DATA_DIR + "/" + zipFileName);
            // delete existing shapefile before upload
            if (shapefileModel.pathExists(userName, shapefilePath)) {
                LOG.info("replacing shapefile '" + shapeName + "'");
                shapefileModel.removeFile(userName, shapefilePath);
            } else {
                LOG.info("adding new shapefile '" + shapeName + "'");
            }
            shapefileModel.fileFromStream(userName, shapefilePath, item.getInputStream());
            return Response.created(URI.create(uriInfo.toString() + "/" + shapeName)).build();
        } catch (WebApplicationException e) {
            throw e;
        } catch (Exception e) {
            throw new WebApplicationException(e,
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(e.getMessage())
                            .type(MediaType.TEXT_PLAIN)
                            .build());
        }
    }



    @GET
    @Path("{name}/content")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getContent(@PathParam("name") String name, @Context HttpServletRequest request, @Context ServletContext context) {
        try {
            ShapefileModel shapefileModel = ShapefileModel.getInstance(context);
            String userName = Utils.getUserName(request, context);
            FileStatus fileStatus = shapefileModel.findShapefile(userName, name);
            if (fileStatus == null) {
                LOG.info("retrieving content of shape zip failed - not found: " + name);
                throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
            }
            LOG.info("retrieving content of shapefile zip " + name);
            StreamingOutput stream = shapefileModel.fileToStream(userName, fileStatus.getPath().toString());
            return Response.ok(stream)
                    .header("Content-Disposition", "attachment; filename=\"" + name + ".zip\"")
                    .build();
        } catch (WebApplicationException e) {
            throw e;
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                      .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    @DELETE
    @Path("/{name}")
    public void delete(@PathParam("name") String name, @Context HttpServletRequest request, @Context ServletContext context) throws NotFoundException {
        try {
            ShapefileModel shapefileModel = ShapefileModel.getInstance(context);
            String username = Utils.getUserName(request, context);
            String shapefilePath = shapefileModel.getUserPath(username, ShapefileModel.REGION_DATA_DIR + "/" + name + ".zip");
            shapefileModel.removeFile(username, shapefilePath);
        } catch (Exception e) {
            throw new WebApplicationException(e, Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }
*/

}
