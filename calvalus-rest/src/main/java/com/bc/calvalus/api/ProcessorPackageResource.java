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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
@Path("processor-packages")
public class ProcessorPackageResource {

//    @Context
//    private ServletContext servletContext;
//    @Context
//    private HttpHeaders httpHeaders;
//    @Context
//    UriInfo uriInfo;
//    @Context
//    Request request;

    static ServiceContainer serviceContainer = null;
    static ServiceContainer getServiceContainer(ServletContext context) throws ServletException, ProductionException {
        if (serviceContainer == null) {
            System.out.println("constructing new service container");
            BackendConfig backendConfig = new BackendConfig(context);
            serviceContainer = new HadoopServiceContainerFactory().create(backendConfig.getConfigMap(), backendConfig.getLocalAppDataDir(), backendConfig.getLocalStagingDir());
        } else {
            System.out.println("using existing service container");
        }
        return serviceContainer;
    }

//    public ProcessorPackageResource(UriInfo uriInfo, Request request, String id) {
//        this.uriInfo = uriInfo;
//        this.request = request;
//        this.id = id;
//    }
//
//    public ProcessorPackageResource() {}
//
    // processor package collection methods

    @GET
    @Path("/")
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    public List<ProcessorPackageEntry> getEntries(@Context HttpServletRequest request, @Context ServletContext context) {
        try {
            String userName = request.getUserPrincipal().getName();
            ServiceContainer serviceContainer = getServiceContainer(context);
            BundleFilter filter = new BundleFilter().withProvider(BundleFilter.PROVIDER_USER).withTheUser(userName);
            BundleDescriptor[] descriptors = serviceContainer.getProductionService().getBundles(userName, filter);
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
            throw new RuntimeException(e);
        }
    }

//    @POST
//    @Path("/")
//    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})  // TODO
//    public ProcessorPackageEntry createEntry() {
//         return null;  // TODO Response.created(uriInfo.getAbsolutePath()).build();
//    }

    @POST
    @Path("/")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response uploadContent(@Context HttpServletRequest request, @Context ServletContext context) {
        try {
            String userName = request.getUserPrincipal().getName();
            ServiceContainer serviceContainer = getServiceContainer(context);
            FileItem item = getFileItem(request);
            String zipFileName = item.getName();
            String bundleId = zipFileName.toLowerCase().endsWith(".zip") ? zipFileName.substring(0, zipFileName.length()-4) : zipFileName;
            String bundlePath = AbstractFileSystemService.getUserPath(userName, "software/" + bundleId);
            // delete existing bundle before upload
            FileSystemService fileSystemService = serviceContainer.getFileSystemService();
            fileSystemService.removeDirectory(userName, bundlePath);
            try (ZipInputStream in = new ZipInputStream(new BufferedInputStream(item.getInputStream(), 64 * 1024))) {
                byte[] buffer = new byte[8192];
                while (true) {
                    ZipEntry entry = in.getNextEntry();
                    if (entry == null) { break; }
                    String fileName = entry.getName();
                    if (fileName.endsWith("/")) { continue; }
                    String filePath = bundlePath + "/" + fileName;
                    try (OutputStream out = new BufferedOutputStream(fileSystemService.addFile(userName, filePath), 64 * 1024)) {
                        int len;
                        while ((len = in.read(buffer)) > 0) {
                            out.write(buffer, 0, len);
                        }
                    }
                }
            }
            return null;  // TODO Response.created(uriInfo.getAbsolutePath()).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
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

//
//    // processor package entry methods
//
//    @GET
//    @Path("{name}/{version}")
//    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_XML})
//    public ProcessorPackageEntry getEntry(@PathParam("name") String name, @PathParam("version") String version) {
//        ProcessorPackageEntry entry = null;
//        if (entry == null) {
//            throw new RuntimeException("Processor " + name + "-" + version + " not found");
//        }
//        return entry;
//    }
//
//    @DELETE
//    @Path("{name}/{version}")
//    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_XML})
//    public void deleteEntry(@PathParam("name") String name, @PathParam("version") String version) {
//        ProcessorPackageEntry entry = null;
//        if (entry == null) {
//            throw new RuntimeException("Processor " + name + "-" + version + " not found");
//        }
//    }
//
//    // processor package content methods
//
//    @GET
//    @Path("{name}/{version}/content")
//    @Produces(MediaType.APPLICATION_OCTET_STREAM)
//    public Response getContent(@PathParam("name") String name, @PathParam("version") String version) {
//        ProcessorPackageEntry entry = null;
//        if (entry == null) {
//            throw new RuntimeException("Processor " + name + "-" + version + " not found");
//        }
//        return null;
//    }
//
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