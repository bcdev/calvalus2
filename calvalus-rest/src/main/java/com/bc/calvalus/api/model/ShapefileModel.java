package com.bc.calvalus.api.model;

import com.bc.calvalus.api.ShapefileEntry;
import com.bc.calvalus.api.Utils;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.inventory.AbstractFileSystemService;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.production.hadoop.HadoopServiceContainerFactory;
import org.apache.hadoop.fs.FileStatus;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class ShapefileModel {

    public static final String REGION_DATA_DIR = "region_data";
    private static Logger LOG = CalvalusLogger.getLogger();
    private static ShapefileModel instance;
    private ServiceContainer serviceContainer;

    public static ShapefileModel getInstance(ServletContext context) throws ServletException, ProductionException {
        if (instance == null) {
            instance = new ShapefileModel(context);
        }
        return instance;
    }

    private ShapefileModel(ServletContext context) throws ServletException, ProductionException {
        LOG.info("constructing new service container");
        BackendConfig backendConfig = new BackendConfig(context);
        serviceContainer = new HadoopServiceContainerFactory().create(backendConfig.getConfigMap(), backendConfig.getLocalAppDataDir(), backendConfig.getLocalStagingDir());
    }

    public List<ShapefileEntry> getShapefiles(String userName) throws IOException {
        List<String> shapefilePathPatterns = new ArrayList<>();
        String shapefilePathPattern = "/calvalus/home/" + userName + "/" + REGION_DATA_DIR + "/.*zip";
        shapefilePathPatterns.add(shapefilePathPattern);
        FileStatus[] fileStatuses = serviceContainer.getFileSystemService().globFiles(userName, shapefilePathPatterns);
        List<ShapefileEntry> shapefileEntries = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses) {
            ShapefileEntry shapefileEntry = new ShapefileEntry(userName, fileStatus.getPath().toString());
            shapefileEntries.add(shapefileEntry);
        }
        return shapefileEntries;
    }

    public void unzipFromStream(String username, String targetPath, InputStream stream) throws IOException {
        Utils.unzipFromStream(username, targetPath, stream, serviceContainer.getFileSystemService());
    }

    public FileStatus getShapefile(String username, String filename) throws IOException {
        List<String> pathPatterns = new ArrayList<>();
        pathPatterns.add("/calvalus/home/" + username + "/" + REGION_DATA_DIR + "/" + filename + ".*");
        return serviceContainer.getFileSystemService().globFiles(username, pathPatterns)[0];
    }

    public String getUserPath(String userName, String path) {
        return AbstractFileSystemService.getUserPath(userName, path);
    }

    public boolean pathExists(String username, String path) throws IOException {
        return serviceContainer.getFileSystemService().pathExists(path, username);
    }

    public boolean removeFile(String userName, String path) throws IOException {
        return serviceContainer.getFileSystemService().removeFile(userName, path);
    }
}
