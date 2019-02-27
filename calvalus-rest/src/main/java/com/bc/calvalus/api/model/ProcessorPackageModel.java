package com.bc.calvalus.api.model;

import com.bc.calvalus.api.Utils;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.inventory.AbstractFileSystemService;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.production.hadoop.HadoopServiceContainerFactory;
import org.apache.hadoop.fs.FileStatus;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.ws.rs.core.StreamingOutput;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class ProcessorPackageModel {
    static Logger LOG = CalvalusLogger.getLogger();

    private static ProcessorPackageModel instance;
    public static ProcessorPackageModel getInstance(ServletContext context) throws ServletException, ProductionException {
        if (instance == null) {
            instance = new ProcessorPackageModel(context);
        }
        return instance;
    }

    private ServiceContainer serviceContainer = null;

    private ProcessorPackageModel(ServletContext context) throws ServletException, ProductionException {
        LOG.info("constructing new service container");
        BackendConfig backendConfig = new BackendConfig(context);
        serviceContainer = new HadoopServiceContainerFactory().create(backendConfig.getConfigMap(), backendConfig.getLocalAppDataDir(), backendConfig.getLocalStagingDir());
    }

    public BundleDescriptor[] getBundleDescriptors(String userName) throws ServletException, ProductionException {
        BundleFilter filter = new BundleFilter().withProvider(BundleFilter.PROVIDER_USER).withTheUser(userName);
        return serviceContainer.getProductionService().getBundles(userName, filter);
    }

    public BundleDescriptor getBundleDescriptor(String userName, String name) throws ServletException, ProductionException {
        for (BundleDescriptor descriptor : getBundleDescriptors(userName)) {
            if ((descriptor.getBundleName() + "-" + descriptor.getBundleVersion()).equals(name)) {
                return descriptor;
            }
        }
        return null;
    }

    public FileStatus[] getFiles(String userName, String bundleName) throws IOException {
        List<String> pathPatterns = Collections.singletonList(AbstractFileSystemService.getUserGlob(userName, "software/" + bundleName));
        return serviceContainer.getFileSystemService().globFiles(userName, pathPatterns);
    }

    public FileStatus getFile(String userName, String userPath) throws IOException {
        List<String> pathPatterns = Collections.singletonList(AbstractFileSystemService.getUserPath(userName, userPath));
        FileStatus[] fileStatuses = serviceContainer.getFileSystemService().globFiles(userName, pathPatterns);
        return fileStatuses.length > 0 ? fileStatuses[0] : null;
    }

    public String getUserPath(String userName, String path) {
        return AbstractFileSystemService.getUserPath(userName, path);
    }

    public boolean pathExists(String username, String path) throws IOException {
        return serviceContainer.getFileSystemService().pathExists(path, username);
    }

    public boolean removeDirectory(String userName, String path) throws IOException {
        return serviceContainer.getFileSystemService().removeDirectory(userName, path);
    }

    public boolean removeFile(String userName, String path) throws IOException {
        return serviceContainer.getFileSystemService().removeFile(userName, path);
    }

    public void invalidateBundleCache() {
        serviceContainer.getProductionService().invalidateBundleCache();
    }

    public String[] globPaths(String userName, List<String> pathPatterns) throws IOException {
        return serviceContainer.getFileSystemService().globPaths(userName, pathPatterns);
    }

    public void unzipFromStream(String username, String targetPath, InputStream stream) throws IOException {
        Utils.unzipFromStream(username, targetPath, stream, serviceContainer.getFileSystemService());
    }

    public StreamingOutput zipToStream(String userName, String[] paths) {
        return outputStream -> {
            ZipOutputStream zip = new ZipOutputStream(new BufferedOutputStream(outputStream, 8192));
            for (String path : paths) {
                System.out.println("adding " + path.substring(path.lastIndexOf('/')+1) + " to zip");
                zip.putNextEntry(new ZipEntry(path.substring(path.lastIndexOf('/')+1)));
                byte[] buffer = new byte[8192];
                int l;
                try (InputStream in = new BufferedInputStream(serviceContainer.getFileSystemService().openFile(userName, path))) {
                    while ((l = in.read(buffer, 0, buffer.length)) > 0) {
                        zip.write(buffer, 0, l);
                    }
                }
                zip.closeEntry();
            }
            zip.finish();
            zip.flush();
        };
    }

    public void fileFromStream(String userName, String filePath, InputStream stream) throws IOException {
        try (BufferedInputStream in = new BufferedInputStream(stream, 8192)) {
            byte[] buffer = new byte[8192];
            try (OutputStream out = new BufferedOutputStream(serviceContainer.getFileSystemService().addFile(userName, filePath), 8192)) {
                int len;
                while ((len = in.read(buffer)) > 0) {
                    out.write(buffer, 0, len);
                }
            }
        }
    }

    public StreamingOutput fileToStream(String userName, String path) {
        return outputStream -> {
            BufferedOutputStream out = new BufferedOutputStream(outputStream, 8192);
            System.out.println("adding " + path.substring(path.lastIndexOf('/')+1) + " to stream");
            byte[] buffer = new byte[8192];
            int l;
            try (InputStream in = new BufferedInputStream(serviceContainer.getFileSystemService().openFile(userName, path))) {
                while ((l = in.read(buffer, 0, buffer.length)) > 0) {
                    out.write(buffer, 0, l);
                }
            }
            out.flush();
        };
    }
}
