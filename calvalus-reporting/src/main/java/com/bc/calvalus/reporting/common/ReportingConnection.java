package com.bc.calvalus.reporting.common;

import static com.bc.calvalus.reporting.common.NullUsageStatistic.NOT_FOUND_STRING_IDENTIFIER;

import com.bc.calvalus.commons.CalvalusLogger;
import com.google.gson.Gson;
import org.esa.snap.core.util.StringUtils;

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
 * This is a class that handles sending requests to Calvalus Reporting server.
 *
 * @author Martin Boettcher
 * @author Hans Permana
 */
public class ReportingConnection {

    private static final Logger LOGGER = CalvalusLogger.getLogger();
    private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static final int DELAY_SECONDS = 300;
    private static final String DEFAULT_CURSOR = "2017-10-01T00:00:00Z";

    private final Reporter reporter;
    private final Gson gson;

    private String cursor;

    public ReportingConnection(Reporter reporter) {
        this.reporter = reporter;
        gson = new Gson();
        cursor = reporter.getConfig().getProperty("reporting.cursor", DEFAULT_CURSOR);
    }

    /**
     * Retrieve a single job report based on the  specified job id
     *
     * @param report The job report (that consists of job id) to be retrieved
     */
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
                reporter.getExecutorService().schedule(report, 60, TimeUnit.SECONDS);
                return;
            }
            String resources = builder.get(String.class);
            if (resources.contains("\"Status\": \"Failed\"")) {
                LOGGER.warning("retrieving resources " + url + " ... failed: " + resources + " - re-scheduling ...");
                report.state = State.NOT_YET_RETRIEVED;
                reporter.getStatusHandler().setFailed(report.job, report.creationTime);
                reporter.getExecutorService().schedule(report, 60, TimeUnit.SECONDS);
                return;
            }
            report.usageStatistic = gson.fromJson(resources, UsageStatistic.class);
            if (report.usageStatistic.getJobId().equalsIgnoreCase("not-found")) {
                LOGGER.info("retrieving resources " + url + " ... not yet available - re-scheduling ...");
                report.state = State.NOT_YET_RETRIEVED;
                reporter.getStatusHandler().setFailed(report.job, report.creationTime);
                reporter.getExecutorService().schedule(report, 60, TimeUnit.SECONDS);
                return;
            }
            LOGGER.info("retrieving resources " + url + " ... done");
            report.state = State.RETRIEVED;
            reporter.getExecutorService().execute(report);
        } catch (RuntimeException e) {
            LOGGER.warning("retrieving resources " + url + " ... failed: " + e.getMessage() + " - re-scheduling ...");
            report.state = State.NOT_YET_RETRIEVED;
            reporter.getStatusHandler().setFailed(report.job, report.creationTime);
            reporter.getExecutorService().schedule(report, 60, TimeUnit.SECONDS);
        }
    }

    /**
     * Checks Calvalus Reporting server periodically to see if there are new records taking account the latest
     * retrieved job.
     */
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
                if (!NOT_FOUND_STRING_IDENTIFIER.equalsIgnoreCase(usageStatistic.getJobId())) {
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
                        reporter.getExecutorService().execute(report);
                        reporter.getStatusHandler().setRunning(report.job, report.creationTime);
                    }
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
            if (StringUtils.isNullOrEmpty(reportContent)) {
                return;
            }
            String[] reportContentLines = reportContent.split("\n");
            String reportContentLastLine = reportContentLines[reportContentLines.length - 1];
            String latestDateTime = reportContentLastLine.split("\t")[1];
            if (cursor.equalsIgnoreCase(latestDateTime)) {
                return;
            }
            cursor = latestDateTime;
            LOGGER.info("cursor updated to " + this.cursor);
        } catch (IOException exception) {
            LOGGER.log(Level.WARNING, "Unable to read report file '" + reportFileName + "'.", exception);
        }
    }
}
