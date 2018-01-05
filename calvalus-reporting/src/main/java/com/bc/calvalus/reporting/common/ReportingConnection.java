package com.bc.calvalus.reporting.common;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.restservice.ws.UsageStatistic;
import com.bc.calvalus.reporting.urban.reporting.CalvalusReport;
import com.google.gson.Gson;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class ReportingConnection {

    private static final Logger LOGGER = CalvalusLogger.getLogger();
    private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static final int DELAY_SECONDS = 300;
    private Gson gson = new Gson();
    private Reporter reporter;
    private String cursor;

    public ReportingConnection(Reporter reporter) {
        this.reporter = reporter;
        cursor = reporter.getConfig().getProperty("reporting.cursor");
    }

    public void retrieveSingle(Report report) {
        String url = String.format("%s/job/%s/%s",
                                   reporter.getConfig().getProperty("reporting.calvalus.url"),
                                   report.job,
                                   report.creationTime.substring(0, "yyyy-MM-dd".length()));
        LOGGER.info("retrieving resources " + url + " ...");
        try {
            Invocation.Builder builder = ClientBuilder.newClient().target(url).request();
            Response response = builder.accept(MediaType.APPLICATION_JSON_TYPE).get();
            int status = response.getStatus();
            if (status < 200 || status >= 300) {
                LOGGER.warning("retrieving report " + url + " ... failed with HTTP " + status + " - re-scheduling ...");
                report.state = State.NOT_YET_RETRIEVED;
                reporter.getStatusHandler().setFailed(report.job, report.creationTime);
                reporter.getTimer().schedule(report, 60, TimeUnit.SECONDS);
                return;
            }
            String resources = builder.get(String.class);
            if (resources.contains("\"Status\": \"Failed\"")) {
                LOGGER.warning("retrieving resources " + url + " ... failed: " + resources + " - re-scheduling ...");
                report.state = State.NOT_YET_RETRIEVED;
                reporter.getStatusHandler().setFailed(report.job, report.creationTime);
                reporter.getTimer().schedule(report, 60, TimeUnit.SECONDS);
                return;
            }
            report.usageStatistics = gson.fromJson(resources, CalvalusReport.class);
            if (report.usageStatistics.getJobId().equalsIgnoreCase("not-found")) {
                LOGGER.info("retrieving resources " + url + " ... not yet available - re-scheduling ...");
                report.state = State.NOT_YET_RETRIEVED;
                reporter.getStatusHandler().setFailed(report.job, report.creationTime);
                reporter.getTimer().schedule(report, 60, TimeUnit.SECONDS);
                return;
            }
            LOGGER.info("retrieving resources " + url + " ... done");
            report.state = State.RETRIEVED;
            reporter.getTimer().execute(report);
        } catch (RuntimeException e) {
            LOGGER.warning("retrieving resources " + url + " ... failed: " + e.getMessage() + " - re-scheduling ...");
            report.state = State.NOT_YET_RETRIEVED;
            reporter.getStatusHandler().setFailed(report.job, report.creationTime);
            reporter.getTimer().schedule(report, 60, TimeUnit.SECONDS);
        }
    }

    public void pollReportingServer() {
        while (true) {
            updateCursor();
            String url = String.format("%s/date/%s",
                                       reporter.getConfig().getProperty("reporting.calvalus.url"),
                                       cursor);
            LOGGER.info("retrieving new records from Reporting server " + url);
            Invocation.Builder builder = ClientBuilder.newClient().target(url).request();
            Response response = builder.accept(MediaType.APPLICATION_JSON_TYPE).get();
            int status = response.getStatus();
            if (status < 200 || status >= 300) {
                LOGGER.warning(
                            "failed to retrieve new records from '" + url + "'. Retrying in " + DELAY_SECONDS + "seconds.");
                return;
            }
            String resources = builder.get(String.class);
            Gson gson = new Gson();
            UsageStatistic[] usageStatistics = gson.fromJson(resources, UsageStatistic[].class);
            List<UsageStatistic> usageStatisticList = Arrays.asList(usageStatistics);
            usageStatisticList.sort(new Comparator<UsageStatistic>() {
                @Override
                public int compare(UsageStatistic o1, UsageStatistic o2) {
                    return Long.compare(o1.getFinishTime(), o2.getFinishTime());
                }
            });
            for (UsageStatistic usageStatistic : usageStatistics) {
                LOGGER.info(String.format("%s\t%s", usageStatistic.getJobId(), usageStatistic.getFinishTime()));
                Report report = new Report(reporter,
                                           usageStatistic.getJobId(),
                                           TIME_FORMAT.format(new Date(usageStatistic.getFinishTime())),
                                           usageStatistic.getJobName(),
                                           usageStatistic.getJobId(),
                                           usageStatistic.getState(),
                                           ""
                );
                if (reporter.getStatusHandler().isHandled(report.job)) {
                    LOGGER.info("skipping " + report.job);
                } else if (cursor != null && report.creationTime.compareTo(cursor) < 0) {
                    LOGGER.info("skipping " + report.job + " with " + report.creationTime + " before cursor " + cursor);
                } else {
                    LOGGER.info("record " + report.job + " received");
                    reporter.getTimer().execute(report);
                    reporter.getStatusHandler().setRunning(report.job, report.creationTime);
                }
            }

            try {
                Thread.sleep(DELAY_SECONDS * 1000);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private void updateCursor() {
        String reportFileName = reporter.getConfig().getProperty("name", reporter.getName() + ".report");
        Path reportFilePath = Paths.get(reportFileName);
        if (!Files.exists(reportFilePath)) {
            LOGGER.log(Level.WARNING, "Report file '" + reportFileName + "' not available.");
            return;
        }
        try {
            String reportContent = new String(Files.readAllBytes(reportFilePath));
            String[] reportContentLines = reportContent.split("\n");
            String reportContentLastLine = reportContentLines[reportContentLines.length - 1];
            cursor = reportContentLastLine.split("\t")[1];
            LOGGER.info("cursor updated to " + cursor);
        } catch (IOException exception) {
            LOGGER.log(Level.WARNING, "Unable to read report file '" + reportFileName + "'.", exception);
        }
    }
}
