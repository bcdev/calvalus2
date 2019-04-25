package com.bc.calvalus.api.model;

import com.bc.calvalus.api.ShapefileEntry;
import com.bc.calvalus.api.Utils;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.inventory.AbstractFileSystemService;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.production.hadoop.HadoopServiceContainerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.security.UserGroupInformation;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.ws.rs.core.StreamingOutput;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

public class ShapefileModel extends FileBasedModel {

    public static final String REGION_DATA_DIR = "region_data";
    private static ShapefileModel instance;

    private ShapefileModel(ServletContext context) throws ServletException, ProductionException {
        super(context);
    }

    public static ShapefileModel getInstance(ServletContext context) throws ServletException, ProductionException {
        if (instance == null) {
            instance = new ShapefileModel(context);
        }
        return instance;
    }

    public String getName(String zipFileName) {
        return zipFileName.endsWith(".zip") ? zipFileName.substring(0, zipFileName.length() - 4) : zipFileName;
    }

    public FileStatus findShapefile(String username, String filename) throws IOException {
        List<String> pathPatterns = new ArrayList<>();
        pathPatterns.add("/calvalus/home/" + username + "/" + REGION_DATA_DIR + "/" + filename + ".zip");
        FileStatus[] fileStatuses = serviceContainer.getFileSystemService().globFiles(username, pathPatterns);
        if (fileStatuses == null || fileStatuses.length == 0) {
            return null;
        }
        return fileStatuses[0];
    }

    public List<ShapefileEntry> getShapefiles(String userName) throws IOException, InterruptedException {
        final List<String> shapefilePathPatterns = Arrays.asList(new String[] { "/calvalus/home/" + userName + "/" + REGION_DATA_DIR + "/.*zip" });
        final FileStatus[] fileStatuses = serviceContainer.getFileSystemService().globFiles(userName, shapefilePathPatterns);
        final List<ShapefileEntry> accu = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses) {
            try {
                accu.add(getShapefileEntry(fileStatus, userName));
            } catch (Exception e) {
                LOG.warning("skipping shapefile " + fileStatus.getPath().getName() + ", reading failed: " + e.getMessage());
            }
        }
        return accu;
    }

    public ShapefileEntry getShapefileEntry(FileStatus fileStatus, String userName) throws IOException, InterruptedException {
        UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(userName);
        return remoteUser.doAs((PrivilegedExceptionAction<ShapefileEntry>) () -> {
            final String zipFileName = fileStatus.getPath().getName();
            final String shapeName = getName(zipFileName);
            final String url = serviceContainer.getFileSystemService().getQualifiedPath(userName, fileStatus.getPath().toString());
            final String[][] data = serviceContainer.getProductionService().loadRegionDataInfo(userName, url);
            final String[] attributes = data[0];
            final String[][] features = Arrays.copyOfRange(data, 1, data.length);
            return new ShapefileEntry(shapeName, userName, new Date(fileStatus.getModificationTime()), fileStatus.getLen(), fileStatus.getPath().toString(), attributes, features);
        });
    }

    public List<ShapefileEntry> getShapeFileBrief(String userName) throws IOException {
        final List<String> shapefilePathPatterns = Arrays.asList(new String[] { "/calvalus/home/" + userName + "/" + REGION_DATA_DIR + "/.*zip" });
        final FileStatus[] fileStatuses = serviceContainer.getFileSystemService().globFiles(userName, shapefilePathPatterns);
        final List<ShapefileEntry> accu = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses) {
            accu.add(new ShapefileEntry(getName(fileStatus.getPath().getName()), userName, new Date(fileStatus.getModificationTime()), fileStatus.getLen(), fileStatus.getPath().toString()));
        }
        return accu;
    }
}
