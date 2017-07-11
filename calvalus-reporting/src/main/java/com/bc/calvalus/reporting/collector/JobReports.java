package com.bc.calvalus.reporting.collector;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.collector.exception.JobReportsException;
import com.bc.calvalus.reporting.collector.exception.JobReportsFileException;
import com.bc.calvalus.reporting.collector.types.JobDetailType;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;


/**
 * @author hans
 */
public class JobReports {

    private static final Logger LOGGER = CalvalusLogger.getLogger();
    private static final String REPORT_FILE_PREFIX = PropertiesWrapper.get("reporting.file.prefix");
    private static final String REPORT_FILE_EXTENSION = PropertiesWrapper.get("reporting.file.extension");
    private static final String REPORT_KEY_PATTERN = "%s-%s";
    private static final String FILE_DATE_INFO_PATTERN = "%04d-%02d-01-to-%04d-%02d-%02d";

    private HashSet<String> knownJobIdSet = new HashSet<>();
    private Map<String, BufferedWriter> writers;
    private String reportingDirectory = PropertiesWrapper.get("reporting.folder.path");

    public void add(String jobId, long finishTime, String jobDetailJson) throws JobReportsException {
        LOGGER.info("Add job '" + jobId + "' to the job reports file.");
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(finishTime);
        String writerKey = String.format(REPORT_KEY_PATTERN, calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1);
        this.knownJobIdSet.add(jobId);
        try {
            if (!this.writers.containsKey(writerKey)) {
                String dateInfo = generateDateInfoString(calendar);
                String reportName = REPORT_FILE_PREFIX + dateInfo + REPORT_FILE_EXTENSION;
                Path reportPath = Paths.get(this.reportingDirectory, reportName);
                Files.createFile(reportPath);
                FileWriter fileWriter = new FileWriter(reportPath.toFile(), true);
                this.writers.put(writerKey, new BufferedWriter(fileWriter));
            }
            this.writers.get(writerKey).write(jobDetailJson);
            this.writers.get(writerKey).write("," + "\n");
        } catch (IOException exception) {
            LOGGER.log(Level.SEVERE, "Unable to write job '" + jobId + "' to job reports file.", exception);
            throw new JobReportsException("Unable to write job '" + jobId + "' to job reports file.", exception);
        }
    }

    void init(String reportingDirectory) throws JobReportsException {
        this.reportingDirectory = reportingDirectory;
        this.writers = new HashMap<>();
        Path directoryPath = Paths.get(this.reportingDirectory);
        if (!Files.exists(directoryPath)) {
            try {
                Files.createDirectory(directoryPath);
            } catch (IOException exception) {
                LOGGER.log(Level.SEVERE, "Unable to create directory '" + this.reportingDirectory + "'", exception);
                throw new JobReportsException("Unable to create directory '" + this.reportingDirectory + "'", exception);
            }
        }
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directoryPath, "calvalus-reporting*.json")) {
            LOGGER.info("Initializing job reports files inside '" + this.reportingDirectory + "'.");
            for (Path reportFile : directoryStream) {
                String reportFileName = reportFile.getFileName().toString();
                String reportKey;
                try {
                    reportKey = parseReportKey(reportFileName);
                } catch (JobReportsFileException exception) {
                    LOGGER.log(Level.WARNING, exception.getMessage());
                    continue;
                }
                Stream<String> stream = Files.lines(reportFile);
                Gson gson = new Gson();
                stream.forEach((line) -> {
                    String formattedEntry = formatToJson(line);
                    JobDetailType jobDetail = gson.fromJson(formattedEntry, JobDetailType.class);
                    this.knownJobIdSet.add(jobDetail.getJobId());
                });
                stream.close();
                FileWriter fileWriter = new FileWriter(Paths.get(this.reportingDirectory, reportFileName).toFile(), true);
                this.writers.put(reportKey, new BufferedWriter(fileWriter));
            }
        } catch (IOException exception) {
            LOGGER.log(Level.SEVERE, "Unable to initiate job reports file inside '" + this.reportingDirectory + "'", exception);
            throw new JobReportsException("Unable to initiate job reports file inside '" + this.reportingDirectory + "'", exception);
        }
    }

    boolean contains(String jobId) {
        return this.knownJobIdSet.contains(jobId);
    }

    HashSet<String> getKnownJobIdSet() {
        return this.knownJobIdSet;
    }

    void flushBufferedWriters() {
        LOGGER.info("Flushing the buffer(s) to the job reports file.");
        for (String reportKey : this.writers.keySet()) {
            try {
                this.writers.get(reportKey).flush();
            } catch (IOException exception) {
                LOGGER.log(Level.SEVERE, "Unable to flush writer for reportKey '" + reportKey + "'.", exception);
            }
        }
    }

    void closeBufferedWriters() {
        LOGGER.info("Closing the writer(s).");
        for (String reportKey : this.writers.keySet()) {
            try {
                this.writers.get(reportKey).close();
            } catch (IOException exception) {
                LOGGER.log(Level.SEVERE, "Unable to close writer for reportKey '" + reportKey + "'.", exception);
            }
        }
    }

    private String generateDateInfoString(Calendar calendar) {
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1; // because it starts from 0
        int lastDayOfMonth = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
        return String.format(FILE_DATE_INFO_PATTERN,
                             year, month, year, month, lastDayOfMonth);
    }

    private String parseReportKey(String reportFileName) throws JobReportsFileException {
        String dateInfo = reportFileName
                    .replace(REPORT_FILE_PREFIX, "")
                    .replace(REPORT_FILE_EXTENSION, "");
        String[] split = dateInfo.split("-");
        int year1 = Integer.parseInt(split[0]);
        int year2 = Integer.parseInt(split[4]);
        int month1 = Integer.parseInt(split[1]);
        int month2 = Integer.parseInt(split[5]);
        if (month1 != month2 || year1 != year2) {
            throw new JobReportsFileException("Unknown date for report file '" + reportFileName + "'.");
        }
        return String.format(REPORT_KEY_PATTERN, year1, month1);
    }

    private String formatToJson(String line) {
        if (line.endsWith(",")) {
            return line.substring(0, line.length() - 1);
        } else {
            return line;
        }
    }
}
