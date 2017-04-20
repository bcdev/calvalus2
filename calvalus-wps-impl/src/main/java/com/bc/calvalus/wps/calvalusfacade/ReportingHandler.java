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
    private static SimpleDateFormat REPORT_FILENAME_FORMAT = new SimpleDateFormat("'calvalus-wps-'yyyy-MM'.report'");
    private static ReportingHandler reportingHandler;
    private String reportPath;

    static {
        REPORT_FILENAME_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private ReportingHandler(ProductionService productionService, String reportPath) {
        this.reportPath = reportPath;
        CalvalusLogger.getLogger().info("reporting handler created to log into " + reportPath);
    }

    public static ReportingHandler createReportHandler(ProductionService productionService, String reportPath) {
        if (reportingHandler == null) {
            reportingHandler = new ReportingHandler(productionService, reportPath);
        }
        return reportingHandler;
    }

    @Override
    public void update(Observable o, Object arg) {
        Production production = (Production) arg;
        Date stopTime = production.getWorkflow().getStopTime();
        String fileName = REPORT_FILENAME_FORMAT.format(stopTime);
        appendToReport(new File(reportPath, fileName), production);
        CalvalusLogger.getLogger().info("request " + production.getName() + " reported " + production.getProcessingStatus() + " should be logged in " + reportPath);
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
