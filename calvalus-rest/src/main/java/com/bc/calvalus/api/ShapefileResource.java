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
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
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

        String userName = request.getUserPrincipal().getName();

        Response response;
        try {
            ShapefileModel shapefileModel = ShapefileModel.getInstance(context);
            GenericEntity<List<ShapefileEntry>> shapefileList = new GenericEntity<List<ShapefileEntry>>(shapefileModel.getShapefiles(userName)) {};
            int count = shapefileList.getEntity().size();
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
            String userName = request.getUserPrincipal().getName();
            FileItem item = getFileItem(request);
            String filename = getValidFilename(item);
            String shapefileName = filename.substring(0, filename.length() - 4);
            ShapefileModel shapefileModel = ShapefileModel.getInstance(context);
            String shapefilePath = getShapefilePath(userName, filename, shapefileModel);
            // delete existing shapefile before upload
            if (shapefileModel.pathExists(userName, shapefilePath)) {
                LOG.info("replacing shapefile '" + shapefileName + "'");
                removeSafe(userName, shapefileName, shapefileModel, shapefilePath);
            } else {
                LOG.info("adding new shapefile '" + shapefileName + "'");
            }
            shapefileModel.unzipFromStream(userName, shapefilePath, item.getInputStream());
            return Response.created(URI.create(uriInfo.toString() + "/" + shapefileName)).build();
        } catch (Exception e) {
            throw new WebApplicationException(e,
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage())
                            .type(MediaType.TEXT_PLAIN)
                            .build());
        }
    }


    @GET
    @Path("/{filename}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    public FileEntry show(@PathParam("filename") String filename, @Context HttpServletRequest request, @Context SecurityContext securityContext, @Context ServletContext context) throws NotFoundException {
        try {
            String userName = request.getUserPrincipal().getName();
            FileStatus fileStatus = ShapefileModel.getInstance(context).getShapefile(userName, filename);
            if (fileStatus == null) {
                throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
            }
        return new FileEntry(fileStatus.getPath().getName(), fileStatus.getOwner(), new Date(fileStatus.getModificationTime()), fileStatus.getLen());
    } catch (WebApplicationException e) {
            throw e;
        } catch (Exception e) {
            throw new WebApplicationException(e, Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    @DELETE
    @Path("/{filename}")
    public void delete(@PathParam("filename") String filename, @Context HttpServletRequest request, @Context ServletContext context) throws NotFoundException {
        try {
            String username = request.getUserPrincipal().getName();
            ShapefileModel shapefileModel = ShapefileModel.getInstance(context);
            String shapefilePath = getShapefilePath(username, filename, shapefileModel);
            removeSafe(username, filename, shapefileModel, shapefilePath);
        } catch (Exception e) {
            throw new WebApplicationException(e, Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    private static String getShapefilePath(String username, String filename, ShapefileModel shapefileModel) {
        return shapefileModel.getUserPath(username, ShapefileModel.REGION_DATA_DIR + "/" + filename);
    }

    private static void removeSafe(String username, String shapefileName, ShapefileModel shapefileModel, String shapefilePath) {
        boolean removed;
        try {
            removed = shapefileModel.removeFile(username, shapefilePath);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to remove shapefile '" + shapefileName + "'", e);
        }
        if (!removed) {
            throw new IllegalStateException("Unable to remove shapefile '" + shapefileName + "' for unknown reason.");
        }
    }

    private static String getValidFilename(FileItem item) {
        String filename;
        if (item != null) {
            filename = item.getName();
        } else {
            throw new IllegalArgumentException("No valid shapefile chosen for upload.");
        }
        if (!filename.endsWith(".zip")) {
            throw new IllegalArgumentException("Shapefile has to be delivered as zip file.");
        }
        return filename;
    }
}
