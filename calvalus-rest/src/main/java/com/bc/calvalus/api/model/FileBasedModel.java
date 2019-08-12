package com.bc.calvalus.api.model;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.inventory.AbstractFileSystemService;
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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class FileBasedModel {

    protected static Logger LOG = CalvalusLogger.getLogger();
    protected ServiceContainer serviceContainer;

    protected FileBasedModel(ServletContext context) throws ServletException, ProductionException {
        LOG.info("constructing new service container");
        BackendConfig backendConfig = BackendConfig.getConfig(context);
        serviceContainer = new HadoopServiceContainerFactory().create(backendConfig.getConfigMap(), backendConfig.getLocalAppDataDir(), backendConfig.getLocalStagingDir());
    }


    public String getUserPath(String userName, String path) {
        return AbstractFileSystemService.getUserPath(userName, path);
    }

    public boolean pathExists(String username, String path) throws IOException {
        return serviceContainer.getFileSystemService().pathExists(path, username);
    }

    public String[] globPaths(String userName, List<String> pathPatterns) throws IOException {
        return serviceContainer.getFileSystemService().globPaths(userName, pathPatterns);
    }

    public boolean removeDirectory(String userName, String path) throws IOException {
        return serviceContainer.getFileSystemService().removeDirectory(userName, path);
    }

    public boolean removeFile(String userName, String path) throws IOException {
        return serviceContainer.getFileSystemService().removeFile(userName, path);
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

    public void unzipFromStream(String username, String targetPath, InputStream stream) throws IOException {
        try (ZipInputStream in = new ZipInputStream(new BufferedInputStream(stream, 8192))) {
            byte[] buffer = new byte[8192];
            while (true) {
                ZipEntry entry = in.getNextEntry();
                if (entry == null) {
                    break;
                }
                String fileName = entry.getName();
                if (fileName.endsWith("/")) {
                    continue;
                }
                LOG.info(String.format("extracting '%s from zip to '%s'", fileName, targetPath));
                String filePath = targetPath + "/" + fileName;
                try (OutputStream out = new BufferedOutputStream(serviceContainer.getFileSystemService().addFile(username, filePath), 8192)) {
                    int len;
                    while ((len = in.read(buffer)) > 0) {
                        out.write(buffer, 0, len);
                    }
                }
            }
        }
    }

}
