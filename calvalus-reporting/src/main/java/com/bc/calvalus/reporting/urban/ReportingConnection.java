package com.bc.calvalus.reporting.urban;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.urban.reporting.CalvalusReport;
import com.google.gson.Gson;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class ReportingConnection {
    static final Logger LOGGER = CalvalusLogger.getLogger();
    private Gson gson = new Gson();
    private UrbanTepReporting reporter;

    ReportingConnection(UrbanTepReporting reporter) {
        this.reporter = reporter;
    }

    void retrieve(Report report) {
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
}
