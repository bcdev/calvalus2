package com.bc.calvalus.reporting.collector;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.collector.jobs.JobDetailType;
import com.google.gson.Gson;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

;

/**
 * @author hans
 */
public class JobReports {

    private static final Logger LOGGER = CalvalusLogger.getLogger();

    private HashSet<String> knownJobIdSet = new HashSet<>();

    public void init(String reportPath) {
        try {
            Stream<String> stream = Files.lines(Paths.get(reportPath));
            Gson gson = new Gson();
            stream.forEach((line) -> {
                String formattedEntry = formatToJson(line);
                JobDetailType jobDetail = gson.fromJson(formattedEntry, JobDetailType.class);
                knownJobIdSet.add(jobDetail.getJobId());
            });
        } catch (IOException exception) {
            LOGGER.log(Level.SEVERE, "Unable to open job reports file '" + reportPath + "'", exception);
        }
    }

    public boolean contains(String jobId) {
        return this.knownJobIdSet.contains(jobId);
    }

    public void add(String jobId, String jobDetailJson) {

    }

    private String formatToJson(String line) {
        if (line.endsWith(",")) {
            return line.substring(0, line.length() - 1);
        } else {
            return line;
        }
    }
}
