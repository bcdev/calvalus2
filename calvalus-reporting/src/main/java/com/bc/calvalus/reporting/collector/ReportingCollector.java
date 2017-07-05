package com.bc.calvalus.reporting.collector;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.extractor.Launcher;
import com.bc.wps.utilities.PropertiesWrapper;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class ReportingCollector {

    private static final Logger LOGGER = CalvalusLogger.getLogger();

    public static void main(String[] args) {
        try {
            PropertiesWrapper.loadConfigFile("calvalus-reporting.properties");
            new ReportingCollector().run();
        } catch (Exception exception) {
            LOGGER.severe("ReportingCollector start failed: " + exception.getMessage());
            System.exit(1);
        }
    }

    private void run() {
        LOGGER.log(Level.INFO, "####################################");
        LOGGER.log(Level.INFO, "#                TEST              #");
        LOGGER.log(Level.INFO, "####################################");
        System.out.println(PropertiesWrapper.get("write.log.interval"));
        System.out.println(PropertiesWrapper.get("calvalus.history.collect.interval"));
    }

    private void startJob() {
        int intervalInMinutes = PropertiesWrapper.getInteger("calvalus.history.collect.interval");

        LOGGER.log(Level.INFO, "################################################");
        LOGGER.log(Level.INFO, "#                START EXTRACTING              #");
        LOGGER.log(Level.INFO, "################################################");

        Launcher.builder().setTimeIntervalInMinutes(intervalInMinutes).start();
    }
}
