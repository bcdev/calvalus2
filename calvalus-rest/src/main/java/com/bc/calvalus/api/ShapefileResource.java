package com.bc.calvalus.api;

import com.bc.calvalus.api.model.ShapefileModel;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.ProductionException;
import com.sun.jersey.api.NotFoundException;
import org.apache.commons.fileupload.FileItem;
import org.apache.hadoop.fs.FileStatus;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.bc.calvalus.api.Utils.getFileItem;

@Path("/shapefiles")
public class ShapefileResource {

    private static Logger LOG = CalvalusLogger.getLogger();

    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    public Response list(@Context HttpServletRequest request, @Context SecurityContext securityContext, @Context ServletContext context) throws NotFoundException {

        Response response;
        try {
            ShapefileModel shapefileModel = ShapefileModel.getInstance(context);
            String userName = Utils.getUserName(request, context);
            List<ShapefileEntry> shapefileList = shapefileModel.getShapeFileBrief(userName);
            int count = shapefileList.size();
            response = Response.ok(shapefileList).header("X-Total-Count", count).build();
        } catch (IOException | ProductionException | ServletException e) {
            LOG.log(Level.WARNING, e.getMessage(), e);
            throw new WebApplicationException(e,
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(e.getMessage())
                            .type(MediaType.TEXT_PLAIN)
                            .build());
        }
        return response;
    }


    @POST
    @Consumes({MediaType.MULTIPART_FORM_DATA})
    public Response upload(@Context HttpServletRequest request, @Context UriInfo uriInfo, @Context ServletContext context) throws NotFoundException {
        try {
            ShapefileModel shapefileModel = ShapefileModel.getInstance(context);
            final String userName = Utils.getUserName(request, context);
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
    @Path("/{name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    public ShapefileEntry show(@PathParam("name") String name, @Context HttpServletRequest request, @Context SecurityContext securityContext, @Context ServletContext context) throws NotFoundException {
        try {
            String userName = Utils.getUserName(request, context);
            ShapefileModel shapefileModel = ShapefileModel.getInstance(context);
            FileStatus fileStatus = shapefileModel.findShapefile(userName, name);
            if (fileStatus == null) {
                throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
            }
            return shapefileModel.getShapefileEntry(fileStatus, userName);
        } catch (WebApplicationException e) {
            throw e;
        } catch (Exception e) {
            throw new WebApplicationException(e, Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
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

}
