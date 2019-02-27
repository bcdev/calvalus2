package com.bc.calvalus.api;

import com.bc.calvalus.api.model.ProcessorPackageModel;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.production.ProductionException;
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
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
@Path("processor-packages")
public class ProcessorPackageResource {

    static Logger LOG = CalvalusLogger.getLogger();
    static int MAX_LIMIT = 200;

    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    public Response list(@Context HttpServletRequest request, @Context ServletContext context,
                                            @QueryParam("offset") Integer offset, @QueryParam("limit") Integer limit) {
        try {
            List<ProcessorPackageEntry> accu = new ArrayList<>();
            String userName = request.getUserPrincipal().getName();
            BundleDescriptor[] descriptors = ProcessorPackageModel.getInstance(context).getBundleDescriptors(userName);
            int start = offset == null ? 0 : offset < descriptors.length ? offset : descriptors.length;
            int stop = limit == null || limit > MAX_LIMIT ? start + MAX_LIMIT : start + limit;
            LOG.info("listing processor packages: " + start + "/" + (stop-start) + "/" + descriptors.length);
            for (int i=start; i<stop && i<descriptors.length; ++i) {
                BundleDescriptor descriptor = descriptors[i];
                accu.add(new ProcessorPackageEntry(descriptor.getBundleName(),
                                                   descriptor.getBundleVersion(),
                                                   descriptor.getOwner(),
                                                   descriptor.getBundleLocation()));
            }
            return Response.ok(accu).header("X-Total-Count", descriptors.length).build();
        } catch (ProductionException|ServletException e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                      .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response uploadContent(@Context HttpServletRequest request, @Context ServletContext context, @Context UriInfo uriInfo) {
        try {
            String userName = request.getUserPrincipal().getName();
            FileItem item = Utils.getFileItem(request);
            String zipFileName = item.getName();
            String bundleId = zipFileName.toLowerCase().endsWith(".zip") ? zipFileName.substring(0, zipFileName.length()-4) : zipFileName;
            String bundlePath = ProcessorPackageModel.getInstance(context).getUserPath(userName, "software/" + bundleId);
            // delete existing bundle before upload
            if (ProcessorPackageModel.getInstance(context).pathExists(userName, bundlePath)) {
                LOG.info("replacing processor package " + bundleId);
                ProcessorPackageModel.getInstance(context).removeDirectory(userName, bundlePath);
            } else {
                LOG.info("adding new processor package " + bundleId);
            }
            ProcessorPackageModel.getInstance(context).invalidateBundleCache();
            ProcessorPackageModel.getInstance(context).unzipFromStream(userName, bundlePath, item.getInputStream());
            return Response.created(URI.create(uriInfo.toString() + "/" + bundleId)).build();
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                      .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    @GET
    @Path("{name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML})
    public ProcessorPackageEntry getEntry(@PathParam("name") String name, @Context HttpServletRequest request, @Context ServletContext context) {
        try {
            LOG.info("showing processor package " + name);
            String userName = request.getUserPrincipal().getName();
            BundleDescriptor descriptor = ProcessorPackageModel.getInstance(context).getBundleDescriptor(userName, name);
            if (descriptor == null) {
                throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
            }
            return new ProcessorPackageEntry(descriptor.getBundleName(),
                                             descriptor.getBundleVersion(),
                                             descriptor.getOwner(),
                                             descriptor.getBundleLocation());
        } catch (WebApplicationException e) {
            throw e;
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                      .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    @DELETE
    @Path("{name}")
    public void deleteEntry(@PathParam("name") String name, @Context HttpServletRequest request, @Context ServletContext context) {
        try {
            LOG.info("deleting processor package " + name);
            String userName = request.getUserPrincipal().getName();
            String bundlePath = ProcessorPackageModel.getInstance(context).getUserPath(userName, "software/" + name);
            ProcessorPackageModel.getInstance(context).removeDirectory(userName, bundlePath);
            ProcessorPackageModel.getInstance(context).invalidateBundleCache();
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                      .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    @GET
    @Path("{name}/content")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getContent(@PathParam("name") String name, @Context HttpServletRequest request, @Context ServletContext context) {
        try {
            String userName = request.getUserPrincipal().getName();
            String bundlePath = ProcessorPackageModel.getInstance(context).getUserPath(userName, "software/" + name);
            if (! ProcessorPackageModel.getInstance(context).pathExists(userName, bundlePath)) {
                LOG.info("retrieving content of processor package failed - not found: " + name);
                throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
            }
            LOG.info("retrieving content of processor package " + name);
            List<String> pathPatterns = new ArrayList<>();
            pathPatterns.add(bundlePath + "/.*");
            final String[] paths = ProcessorPackageModel.getInstance(context).globPaths(userName, pathPatterns);
            System.out.println(paths.length + " found in " + bundlePath);
            StreamingOutput stream = ProcessorPackageModel.getInstance(context).zipToStream(userName, paths);
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


    @GET
    @Path("{name}/files")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML})
    public Response getFileEntries(@Context HttpServletRequest request, @Context ServletContext context,
                                            @PathParam("name") String name,
                                            @QueryParam("offset") Integer offset, @QueryParam("limit") Integer limit) {
        try {
            List<FileEntry> accu = new ArrayList<>();
            String userName = request.getUserPrincipal().getName();
            BundleDescriptor descriptor = ProcessorPackageModel.getInstance(context).getBundleDescriptor(userName, name);
            if (descriptor == null) {
                throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
            }
            FileStatus[] files = ProcessorPackageModel.getInstance(context).getFiles(userName, name);
            int start = offset == null ? 0 : offset < files.length ? offset : files.length;
            int stop = limit == null || limit > MAX_LIMIT ? start + MAX_LIMIT : start + limit;
            LOG.info("listing files: " + start + "/" + (stop-start) + "/" + files.length);
            for (int i=start; i<stop && i<files.length; ++i) {
                accu.add(new FileEntry(files[i].getPath().getName(), files[i].getOwner(), new Date(files[i].getModificationTime()), files[i].getLen()));
            }
            return Response.ok(accu).header("X-Total-Count", files.length).build();
        } catch (WebApplicationException e) {
            throw e;
        } catch (Exception e) {
            LOG.log(Level.WARNING, e.getMessage(), e);
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                      .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    @POST
    @Path("{name}/files")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response putFile(@Context HttpServletRequest request, @Context ServletContext context, @Context UriInfo uriInfo,
                            @PathParam("name") String name) {
        try {
            String userName = request.getUserPrincipal().getName();
            FileItem item = Utils.getFileItem(request);
            String fileName = item.getName();
            String filePath = ProcessorPackageModel.getInstance(context).getUserPath(userName, "software/" + name + "/" + fileName);
            // delete existing file before upload
            if (ProcessorPackageModel.getInstance(context).pathExists(userName, filePath)) {
                LOG.info("replacing processor file " + name + "/" + fileName);
                ProcessorPackageModel.getInstance(context).removeDirectory(userName, filePath);
            } else {
                LOG.info("adding processor file " + name + "/" + fileName);
            }
            ProcessorPackageModel.getInstance(context).fileFromStream(userName, filePath, item.getInputStream());
            return Response.created(URI.create(uriInfo.toString() + "/" + fileName)).build();
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                      .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    @GET
    @Path("{name}/files/{filename}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML})
    public FileEntry getFileEntry(@Context HttpServletRequest request, @Context ServletContext context,
                                  @PathParam("name") String name, @PathParam("filename") String fileName) {
        try {
            String userName = request.getUserPrincipal().getName();
            FileStatus fileStatus = ProcessorPackageModel.getInstance(context).getFile(userName, "software/" + name + "/" + fileName);
            if (fileStatus == null) {
                throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
            }
            return new FileEntry(fileStatus.getPath().getName(), fileStatus.getOwner(), new Date(fileStatus.getModificationTime()), fileStatus.getLen());
        } catch (WebApplicationException e) {
            throw e;
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                      .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    @DELETE
    @Path("{name}/files/{filename}")
    public void deleteFile(@PathParam("name") String name, @PathParam("filename") String fileName,
                           @Context HttpServletRequest request, @Context ServletContext context) {
        try {
            LOG.info("deleting processor file " + name + "/" + fileName);
            String userName = request.getUserPrincipal().getName();
            String filePath = ProcessorPackageModel.getInstance(context).getUserPath(userName, "software/" + name + "/" + fileName);
            ProcessorPackageModel.getInstance(context).removeFile(userName, filePath);
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                      .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    @GET
    @Path("{name}/files/{filename}/content")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getFileContent(@Context HttpServletRequest request, @Context ServletContext context,
                                            @PathParam("name") String name, @PathParam("filename") String fileName) {
        try {
            String userName = request.getUserPrincipal().getName();
            String filePath = ProcessorPackageModel.getInstance(context).getUserPath(userName, "software/" + name + "/" + fileName);
            if (! ProcessorPackageModel.getInstance(context).pathExists(userName, filePath)) {
                throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
            }
            LOG.info("retrieving content of processor file " + name + "/" + fileName);
            StreamingOutput stream = ProcessorPackageModel.getInstance(context).fileToStream(userName, filePath);
            return Response.ok(stream)
                    .header("Content-Disposition", "attachment; filename=\"" + fileName + "\"")
                    .build();
        } catch (WebApplicationException e) {
            throw e;
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                      .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

}