package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionService;
import com.bc.wps.utilities.PropertiesWrapper;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Observable;
import java.util.Observer;

/**
 * TODO add API doc
 *
 * @author Martin
 * @author Muhammad
 */
public class ReportingHandler implements Observer {
    private static final String CALVALUS_WPS_REPORTING = "calvalus-wps-reporting.report";
    private static SimpleDateFormat ISO_TIME_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
    private static ReportingHandler reportingHandler;
    private String reportPath;
    private String reportingStatusUri;

    private ReportingHandler(ProductionService productionService, String reportPath) {
        this.reportPath = reportPath;
        reportingStatusUri = PropertiesWrapper.get("wps.reporting.status.uri");
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
        try {
            Production production = (Production) arg;
            appendToReport(new File(reportPath, CALVALUS_WPS_REPORTING), production);
            CalvalusLogger.getLogger().info("request " + production.getName() + " reported " + production.getProcessingStatus() + " logged in " + reportPath);
        } catch (Exception ex) {
            CalvalusLogger.getLogger().severe(String.format("reporting %s failed: %s", arg, ex.getMessage()));
        }
    }

    private synchronized void appendToReport(File fileToLogIn, Production production) {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileToLogIn, true))) {
            for (Object jobId : production.getJobIds()) {
                bufferedWriter.
                        append("UrbanTepPortal").append('\t').
                        append(String.valueOf(jobId)).append('\t').
                        append(ISO_TIME_FORMAT.format(production.getWorkflow().getSubmitTime())).append('\t').
                        append(production.getName()).append('\t').
                        append(production.getId()).append('\t').
                        append(String.valueOf(production.getProcessingStatus().getState())).append('\t').
                        append(reportingStatusUri).append("?Service=WPS&Request=GetStatus&JobId=").append(String.valueOf(jobId));
                bufferedWriter.newLine();
            }
        } catch (IOException ex) {
            CalvalusLogger.getLogger().severe(String.format("Appending to report %s failed: %s", fileToLogIn, ex.getMessage()));
        }
    }
}
