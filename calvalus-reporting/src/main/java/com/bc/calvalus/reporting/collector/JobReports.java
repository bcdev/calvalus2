package com.bc.calvalus.reporting.collector;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.collector.exception.JobReportsException;
import com.bc.calvalus.reporting.collector.types.JobDetailType;
import com.google.gson.Gson;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;


/**
 * @author hans
 */
public class JobReports {

    private static final Logger LOGGER = CalvalusLogger.getLogger();

    private HashSet<String> knownJobIdSet = new HashSet<>();
    private BufferedWriter writer;

    public void add(String jobId, String jobDetailJson) throws JobReportsException {
        LOGGER.info("Add job '" + jobId + "' to the job reports file.");
        this.knownJobIdSet.add(jobId);
        try {
            this.writer.write(jobDetailJson);
            this.writer.write("\n");
        } catch (IOException exception) {
            LOGGER.log(Level.SEVERE, "Unable to write job '" + jobId + "' to job reports file.", exception);
            throw new JobReportsException("Unable to write job '" + jobId + "' to job reports file.", exception);
        }
    }

    void init(String reportPath) throws JobReportsException {
        LOGGER.info("Initializing job reports file '" + reportPath + "'.");
        try {
            Path path = Paths.get(reportPath);
            if (!Files.exists(path)) {
                Files.createFile(path);
            }
            Stream<String> stream = Files.lines(path);
            Gson gson = new Gson();
            stream.forEach((line) -> {
                String formattedEntry = formatToJson(line);
                JobDetailType jobDetail = gson.fromJson(formattedEntry, JobDetailType.class);
                this.knownJobIdSet.add(jobDetail.getJobId());
            });
            stream.close();
            FileWriter fileWriter = new FileWriter(reportPath, true);
            this.writer = new BufferedWriter(fileWriter);
        } catch (IOException exception) {
            LOGGER.log(Level.SEVERE, "Unable to open job reports file '" + reportPath + "'", exception);
            throw new JobReportsException("Unable to open job reports file '" + reportPath + "'", exception);
        }
    }

    boolean contains(String jobId) {
        return this.knownJobIdSet.contains(jobId);
    }

    HashSet<String> getKnownJobIdSet() {
        return this.knownJobIdSet;
    }

    void flushBufferedWriter() {
        LOGGER.info("Flushing the buffer to the job reports file.");
        try {
            this.writer.flush();
        } catch (IOException exception) {
            LOGGER.log(Level.SEVERE, "Unable to flush the writer", exception);
        }
    }

    void closeBufferedWriter() {
        LOGGER.info("Close the writer.");
        try {
            this.writer.close();
        } catch (IOException exception) {
            LOGGER.log(Level.SEVERE, "Unable to close the writer", exception);
        }
    }

    private String formatToJson(String line) {
        if (line.endsWith(",")) {
            return line.substring(0, line.length() - 1);
        } else {
            return line;
        }
    }
}
