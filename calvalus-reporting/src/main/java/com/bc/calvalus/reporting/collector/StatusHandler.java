package com.bc.calvalus.reporting.collector;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.util.PropertiesWrapper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Logger;

/**
 * @author hans
 */
class StatusHandler {

    private static final Logger LOGGER = CalvalusLogger.getLogger();

    private String statusFileName = PropertiesWrapper.get("name") + ".status";
    private int preExistingJobNumber;
    private int newJobNumber;
    private String serviceStartDate;

    void initReport(int totalJobNumber) {
        this.preExistingJobNumber = totalJobNumber;
        this.newJobNumber = 0;
        this.serviceStartDate = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        writeStatus();
    }

    void updateNewJobNumber(int currentJobNumber){
        this.newJobNumber = currentJobNumber - this.preExistingJobNumber;
        writeStatus();
    }

    private void writeStatus() {
        try {
            Path statusFilePath = Paths.get(statusFileName);
            if (!Files.exists(statusFilePath)) {
                Files.createFile(statusFilePath);
            }

            try (BufferedWriter out = new BufferedWriter(new FileWriter(statusFileName))) {
                // 0 reported, 77 pre-existing since 2017-04-01T00:00:00
                // 10 reported, 77 pre-existing since since 2017-04-01T00:00:00
                out.append(String.valueOf(this.newJobNumber)).append(" reported, ").
                            append(String.valueOf(preExistingJobNumber)).append(" pre-existing since ").
                            append(this.serviceStartDate).append("\n");
            }
        } catch (IOException e) {
            LOGGER.warning("failed writing instance log " + PropertiesWrapper.get("name") + ".status: " + e.getMessage());
        }
    }
}
