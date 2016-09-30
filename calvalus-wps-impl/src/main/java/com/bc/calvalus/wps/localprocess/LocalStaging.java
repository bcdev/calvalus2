package com.bc.calvalus.wps.localprocess;

import com.bc.wps.utilities.PropertiesWrapper;
import com.sun.jersey.api.uri.UriBuilderImpl;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
public class LocalStaging {

    private static final String CATALINA_BASE = System.getProperty("catalina.base");

    protected List<String> getProductUrls(String jobId, String userName, String hostName, int portNumber) {
        Path targetDirectoryPath = Paths.get(CATALINA_BASE + PropertiesWrapper.get("wps.application.path"),
                                             PropertiesWrapper.get("utep.output.directory"), userName, jobId);
        File targetDir = targetDirectoryPath.toFile();
        List<String> resultUrls = new ArrayList<>();
        File[] resultProductFiles = targetDir.listFiles();
        if (resultProductFiles != null) {
            for (File resultProductFile : resultProductFiles) {
                UriBuilder builder = new UriBuilderImpl();
                String productUrl = builder.scheme("http")
                            .host(hostName)
                            .port(portNumber)
                            .path(PropertiesWrapper.get("wps.application.name"))
                            .path(PropertiesWrapper.get("utep.output.directory"))
                            .path(targetDir.getParentFile().getName())
                            .path(targetDir.getName())
                            .path(resultProductFile.getName()).build().toString();
                resultUrls.add(productUrl);
            }

            UriBuilder builder = new UriBuilderImpl();
            String metadataUrl = builder.scheme("http")
                        .host(hostName)
                        .port(portNumber)
                        .path(PropertiesWrapper.get("wps.application.name"))
                        .path(PropertiesWrapper.get("utep.output.directory"))
                        .path(targetDir.getParentFile().getName())
                        .path(targetDir.getName())
                        .path(jobId + "-metadata")
                        .build().toString();
            resultUrls.add(metadataUrl);
        }

        return resultUrls;
    }
}
