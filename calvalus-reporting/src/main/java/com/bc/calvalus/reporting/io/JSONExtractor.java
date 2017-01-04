package com.bc.calvalus.reporting.io;

import com.bc.calvalus.reporting.ws.UsageStatistic;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
public class JSONExtractor {

    public UsageStatistic getSingleStatistic(String jobId) {
        return null;
    }

    public List<UsageStatistic> getAllStatistics() throws IOException {
        List<Path> filePaths = getFilePaths();
        List<UsageStatistic> usageStatistics = new ArrayList<>();
        for (Path filePath : filePaths) {
            Gson gson = new Gson();
            FileReader reader = new FileReader(filePath.toFile());
            List<UsageStatistic> usages = gson.fromJson(reader,
                                                        new TypeToken<List<UsageStatistic>>() {
                                                        }.getType());
            usageStatistics.addAll(usages);
        }
        return usageStatistics;
    }

    private List<Path> getFilePaths() throws IOException {
        List<Path> filePaths = new ArrayList<>();
        String reportDirName = PropertiesWrapper.get("reporting.directory");
        URL reportDir = getClass().getClassLoader().getResource(reportDirName);
        if (reportDir == null) {
            throw new IOException("Reporting directory '" + reportDirName + "' is not available");
        }
        try {
            Files.walkFileTree(Paths.get(reportDir.toURI()), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (file.getFileName().toString().toLowerCase().endsWith(".json")) {
                        filePaths.add(file);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (URISyntaxException exception) {
            throw new IOException("Reporting directory '" + reportDirName + "' is not available");
        }
        return filePaths;
    }
}
