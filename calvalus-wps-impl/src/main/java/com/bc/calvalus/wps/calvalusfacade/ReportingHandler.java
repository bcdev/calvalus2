package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionService;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.TimeZone;
import java.util.logging.Level;

/**
 * TODO add API doc
 *
 * @author Martin
 * @author Muhammad
 */
public class ReportingHandler implements Observer {
    private static SimpleDateFormat FILE_NAME_FORMAT = new SimpleDateFormat("'calvalus-wps-'yyyy-MM'.report'");
    private static ReportingHandler reportingHandler;
    private String reportPathString;

    static {
        FILE_NAME_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private ReportingHandler(ProductionService productionService, Map<String, String> config) {
        reportPathString = config.get("wps.reporting.db.path");
        CalvalusLogger.getLogger().info("reporting handler created to log into " + reportPathString);
    }

    public static ReportingHandler createReportHandler(ProductionService productionService, Map<String, String> config) {
        if (reportingHandler == null) {
            reportingHandler = new ReportingHandler(productionService, config);
        }
        return reportingHandler;
    }

    @Override
    public void update(Observable o, Object arg) {
        Production production = (Production) arg;
        Date stopTime = production.getWorkflow().getStopTime();
        String fileName = FILE_NAME_FORMAT.format(stopTime);
        appendToReport(new File(reportPathString, fileName), production);
        CalvalusLogger.getLogger().info("request " + production.getName() + " reported " + production.getProcessingStatus() + " should be logged in " + reportPathString);
    }


    private synchronized void appendToReport(File fileToLogIn, Production production) {
        try (FileWriter fileWriter = new FileWriter(fileToLogIn, true);
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
            bufferedWriter.append(production.getId()).append("#").append(production.getName());
            bufferedWriter.write(",");
            bufferedWriter.newLine();
        } catch (IOException ex) {
            CalvalusLogger.getLogger().log(Level.SEVERE, String.format("Appending to report %s failed: %s",fileToLogIn,ex.getMessage() ));
        }
    }
}
