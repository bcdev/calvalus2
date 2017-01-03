package com.bc.calvalus.reporting.io;

import com.bc.calvalus.reporting.ws.UsageStatistic;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author hans
 */
public class JSONReader {

    public UsageStatistic getSingleStatistic(String jobId) {
        return null;
    }

    public List<UsageStatistic> getAllStatistics() throws IOException {
        List<Path> filePaths = getFilePaths();
        List<UsageStatistic> usageStatistics = new ArrayList<>();
        for (Path filePath : filePaths) {
            String fileContent = new String(Files.readAllBytes(filePath));
            Gson gson = new Gson();
            UsageStatistic[] usages = gson.fromJson(fileContent, UsageStatistic[].class);
            usageStatistics.addAll(Arrays.asList(usages));
        }
        return usageStatistics;
    }

    private List<Path> getFilePaths() throws IOException {
        List<Path> filePaths = new ArrayList<>();
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(PropertiesWrapper.get("reporting.directory")))) {
            for (Path path : directoryStream) {
                filePaths.add(path);
            }
            return filePaths;
        }
    }
}
