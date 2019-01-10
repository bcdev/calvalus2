package com.bc.calvalus.api;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.inventory.AbstractFileSystemService;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.production.hadoop.HadoopServiceContainerFactory;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

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
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
@Path("processor-packages")
public class ProcessorPackageResource {

    static Logger LOG = CalvalusLogger.getLogger();

    static ServiceContainer serviceContainer = null;
    static ServiceContainer getServiceContainer(ServletContext context) throws ServletException, ProductionException {
        if (serviceContainer == null) {
            LOG.info("constructing new service container");
            BackendConfig backendConfig = new BackendConfig(context);
            serviceContainer = new HadoopServiceContainerFactory().create(backendConfig.getConfigMap(), backendConfig.getLocalAppDataDir(), backendConfig.getLocalStagingDir());
        }
        return serviceContainer;
    }

    // processor package collection methods

    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    public List<ProcessorPackageEntry> list(@Context HttpServletRequest request, @Context ServletContext context) {
        try {
            String userName = request.getUserPrincipal().getName();
            ServiceContainer serviceContainer = getServiceContainer(context);
            BundleFilter filter = new BundleFilter().withProvider(BundleFilter.PROVIDER_USER).withTheUser(userName);
            BundleDescriptor[] descriptors = serviceContainer.getProductionService().getBundles(userName, filter);
            LOG.info("listing processor packages: " + descriptors.length);
            List<ProcessorPackageEntry> accu = new ArrayList<>();
            for (BundleDescriptor descriptor : descriptors) {
                accu.add(new ProcessorPackageEntry(descriptor.getBundleName(),
                                                   descriptor.getBundleVersion(),
                                                   descriptor.getOwner(),
                                                   descriptor.getBundleLocation()));
            }
//            accu.add(new ProcessorPackageEntry("somename", "someversion", "someowner", "somelocation"));
            return accu;
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
            ServiceContainer serviceContainer = getServiceContainer(context);
            FileItem item = getFileItem(request);
            String zipFileName = item.getName();
            String bundleId = zipFileName.toLowerCase().endsWith(".zip") ? zipFileName.substring(0, zipFileName.length()-4) : zipFileName;
            String bundlePath = AbstractFileSystemService.getUserPath(userName, "software/" + bundleId);
            // delete existing bundle before upload
            FileSystemService fileSystemService = serviceContainer.getFileSystemService();
            if (fileSystemService.pathExists(bundlePath)) {
                LOG.info("replacing processor package " + bundleId);
                fileSystemService.removeDirectory(userName, bundlePath);
            } else {
                LOG.info("adding new processor package " + bundleId);
            }
            serviceContainer.getProductionService().invalidateBundleCache();
            try (ZipInputStream in = new ZipInputStream(new BufferedInputStream(item.getInputStream(), 8192))) {
                byte[] buffer = new byte[8192];
                while (true) {
                    ZipEntry entry = in.getNextEntry();
                    if (entry == null) { break; }
                    String fileName = entry.getName();
                    if (fileName.endsWith("/")) { continue; }
                    String filePath = bundlePath + "/" + fileName;
                    try (OutputStream out = new BufferedOutputStream(fileSystemService.addFile(userName, filePath), 8192)) {
                        int len;
                        while ((len = in.read(buffer)) > 0) {
                            out.write(buffer, 0, len);
                        }
                    }
                }
            }
            return Response.created(URI.create(uriInfo.toString() + "/" + bundleId)).build();
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                      .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    private FileItem getFileItem(HttpServletRequest request) throws FileUploadException {
        ServletFileUpload upload = new ServletFileUpload(new DiskFileItemFactory());
        List<FileItem> items = upload.parseRequest(request);
        for (FileItem item : items) {
            if (!item.isFormField()) {
                return item;
            }
        }
        return null;
    }


    // processor package entry methods

    @GET
    @Path("{name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML})
    public ProcessorPackageEntry getEntry(@PathParam("name") String name, @Context HttpServletRequest request, @Context ServletContext context) {
        try {
            LOG.info("showing processor package " + name);
            String userName = request.getUserPrincipal().getName();
            ServiceContainer serviceContainer = getServiceContainer(context);
            BundleFilter filter = new BundleFilter().withProvider(BundleFilter.PROVIDER_USER).withTheUser(userName);
            BundleDescriptor[] descriptors = serviceContainer.getProductionService().getBundles(userName, filter);
            for (BundleDescriptor descriptor : descriptors) {
                if ((descriptor.getBundleName() + "-" + descriptor.getBundleVersion()).equals(name)) {
                    return new ProcessorPackageEntry(descriptor.getBundleName(),
                                                     descriptor.getBundleVersion(),
                                                     descriptor.getOwner(),
                                                     descriptor.getBundleLocation());
                }
            }
        } catch (ProductionException|ServletException e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                      .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
        throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
    }

    @DELETE
    @Path("{name}")
    public void deleteEntry(@PathParam("name") String name, @Context HttpServletRequest request, @Context ServletContext context) {
        try {
            LOG.info("deleting processor package " + name);
            String userName = request.getUserPrincipal().getName();
            ServiceContainer serviceContainer = getServiceContainer(context);
            String bundlePath = AbstractFileSystemService.getUserPath(userName, "software/" + name);
            FileSystemService fileSystemService = serviceContainer.getFileSystemService();
            fileSystemService.removeDirectory(userName, bundlePath);
            serviceContainer.getProductionService().invalidateBundleCache();
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                      .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

    // processor package content methods

    @GET
    @Path("{name}/content")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getContent(@PathParam("name") String name, @Context HttpServletRequest request, @Context ServletContext context) {
        try {
            String userName = request.getUserPrincipal().getName();
            ServiceContainer serviceContainer = getServiceContainer(context);
            String bundlePath = AbstractFileSystemService.getUserPath(userName, "software/" + name);
            FileSystemService fileSystemService = serviceContainer.getFileSystemService();
            if (! fileSystemService.pathExists(bundlePath)) {
                LOG.info("retrieving content of processor package failed - not found: " + name);
                throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
            }
            LOG.info("retrieving content of processor package " + name);
            List<String> pathPatterns = new ArrayList<>();
            pathPatterns.add(bundlePath + "/.*");
            final String[] paths = fileSystemService.globPaths(userName, pathPatterns);
            System.out.println(paths.length + " find in " + bundlePath);
            StreamingOutput stream = new StreamingOutput() {
                @Override
                public void write(OutputStream outputStream) throws IOException, WebApplicationException {
                    ZipOutputStream zip = new ZipOutputStream(new BufferedOutputStream(outputStream, 8192));
                    for (String path : paths) {
                        System.out.println("adding " + path.substring(path.lastIndexOf('/')+1) + " to zip");
                        zip.putNextEntry(new ZipEntry(path.substring(path.lastIndexOf('/')+1)));
                        byte[] buffer = new byte[8192];
                        int l;
                        try (InputStream in = new BufferedInputStream(fileSystemService.openFile(userName, path))) {
                            while ((l = in.read(buffer, 0, buffer.length)) > 0) {
                                zip.write(buffer, 0, l);
                            }
                        }
                        zip.closeEntry();
                    }
                    zip.finish();
                    zip.flush();
                }
            };
            Response.ResponseBuilder response = Response.ok(stream);
            response.header("Content-Disposition", "attachment; filename=\"" + name + ".zip\"");
            return response.build();
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                      .entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build());
        }
    }

//    @PUT
//    @Path("{name}/{version}/content")
//    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
//    public Response putContent(@PathParam("name") String name, @PathParam("version") String version) {
//        ProcessorPackageEntry entry = null;
//        if (entry == null) {
//            throw new RuntimeException("Processor " + name + "-" + version + " not found");
//        }
//        return null;
//    }
//
//    // file collection methods
//
//    @GET
//    @Path("{name}/{version}/files")
//    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_XML})
//    public List<FileEntry> getFileEntries(@PathParam("name") String name, @PathParam("version") String version) {
//        ProcessorPackageEntry entry = null;
//        if (entry == null) {
//            throw new RuntimeException("Processor " + name + "-" + version + " not found");
//        }
//        List<FileEntry> fileEntries = null;
//        return fileEntries;
//    }
//
//    // file entry methods
//
//    @GET
//    @Path("{name}/{version}/files/{filename}")
//    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_XML})
//    public FileEntry getFileEntry(@PathParam("name") String name, @PathParam("version") String version, @PathParam("filename") String filename) {
//        ProcessorPackageEntry entry = null;
//        if (entry == null) {
//            throw new RuntimeException("Processor " + name + "-" + version + " not found");
//        }
//        FileEntry fileEntry = null;
//        return fileEntry;
//    }
//
//    @GET
//    @Path("{name}/{version}/files/{filename}/content")
//    @Produces(MediaType.APPLICATION_OCTET_STREAM)
//    public Response getFileContent(@PathParam("name") String name, @PathParam("version") String version, @PathParam("filename") String filename) {
//        ProcessorPackageEntry entry = null;
//        if (entry == null) {
//            throw new RuntimeException("Processor " + name + "-" + version + " not found");
//        }
//        FileEntry fileEntry = null;
//        return null;
//    }
//
//    @PUT
//    @Path("{name}/{version}/files/{filename}/content")
//    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
//    public Response putFile(@PathParam("name") String name, @PathParam("version") String version, @PathParam("filename") String filename) {
//        ProcessorPackageEntry entry = null;
//        if (entry == null) {
//            throw new RuntimeException("Processor " + name + "-" + version + " not found");
//        }
//        return null;
//    }
}