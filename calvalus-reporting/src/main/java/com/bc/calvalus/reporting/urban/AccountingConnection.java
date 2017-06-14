package com.bc.calvalus.reporting.urban;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.urban.account.Account;
import com.bc.calvalus.reporting.urban.account.Any;
import com.bc.calvalus.reporting.urban.account.Compound;
import com.bc.calvalus.reporting.urban.account.Message;
import com.bc.calvalus.reporting.urban.account.Quantity;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class AccountingConnection {

    static final Logger LOGGER = CalvalusLogger.getLogger();
    private final UrbanTepReporting reporter;
    private Client urbantepWebClient = null;
    static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    static {
        TIME_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    AccountingConnection(UrbanTepReporting reporter) {
        this.reporter = reporter;
    }

    void send(Report report) {
        Account account = new Account(reporter.getConfig().getProperty("reporting.urbantep.subsystem"),
                                      report.usageStatistics.getUser().replace("tep_", ""),
                                      report.usageStatistics.getRemoteRef());
        Compound compound = new Compound(report.requestId,
                                         report.usageStatistics.getJobName(),
                                         report.usageStatistics.getProcessType(),
                                         new Any(report.uri));
        List<Quantity> quantityList = Arrays.asList(
                new Quantity("CPU_MILLISECONDS", report.usageStatistics.getCpuMilliseconds()),
                new Quantity("PHYSICAL_MEMORY_BYTES", report.usageStatistics.getCpuMilliseconds() == 0 ? (report.usageStatistics.getMbMillisMapTotal() + report.usageStatistics.getMbMillisReduceTotal()) * 1024 * 1024 : (report.usageStatistics.getMbMillisMapTotal() + report.usageStatistics.getMbMillisReduceTotal()) / report.usageStatistics.getCpuMilliseconds() * 1024 * 1024),
                new Quantity("BYTE_READ", report.usageStatistics.getHdfsBytesRead()),
                new Quantity("BYTE_WRITTEN", report.usageStatistics.getHdfsBytesWritten()),
                new Quantity("PROC_INSTANCE", (long) report.usageStatistics.getMapsCompleted() + report.usageStatistics.getReducesCompleted()),
                new Quantity("NUM_REQ", 1));
        Message message = new Message(report.usageStatistics.getJobId(),
                                      account,
                                      compound,
                                      quantityList,
                                      reporter.getConfig().getProperty("reporting.urbantep.origin"),
                                      TIME_FORMAT.format(new Date(report.usageStatistics.getFinishTime())),
                                      "SUCCEEDED".equals(report.usageStatistics.getState()) ? "NOMINAL" : "DEGRADED");
        String messageJson = message.toJson();
        File file = new File(reporter.getConfig().getProperty("reporting.urbantep.reportsdir"), String.format("account-message-%s.json", report.job));
        try (FileWriter fileWriter = new FileWriter(file)) {
            fileWriter.append(messageJson).append('\n');
        } catch (IOException e) {
            LOGGER.warning("Writing report " + report.job + " to file failed: " + e.getMessage());
            report.state = State.NOT_YET_ACCOUNTED;
            reporter.getStatusHandler().setFailed(report.job, report.creationTime);
            reporter.getTimer().schedule(report, 60, TimeUnit.SECONDS);
            return;
        }
        LOGGER.info(String.format("report %s written to file", report.job));
        if (Boolean.parseBoolean(reporter.getConfig().getProperty("reporting.urbantep.nosending", "false"))) {
            LOGGER.warning("skip sending report " + report.job + " - reporting.urbantep.nosending=true");
            report.state = State.ACCOUNTED;
            reporter.getStatusHandler().setHandled(report.job, report.creationTime);
            return;
        }
        try {
            if (urbantepWebClient == null) {
                urbantepWebClient = ClientBuilder.newClient();
                urbantepWebClient.register(HttpAuthenticationFeature.basicBuilder()
                                                   .nonPreemptive().credentials(reporter.getConfig().getProperty("reporting.urbantep.user"),
                                                                                reporter.getConfig().getProperty("reporting.urbantep.password")).build());
            }
            WebTarget target = urbantepWebClient.target(reporter.getConfig().getProperty("reporting.urbantep.url"));
            Entity<String> jsonEntity = Entity.json(messageJson);
            Invocation.Builder request = target.request(MediaType.TEXT_PLAIN_TYPE);
            Response response = request.post(jsonEntity);
            String reasonPhrase = response.getStatusInfo().getReasonPhrase();
            LOGGER.info("report " + report.job + " sent to Urban TEP accounting: " + reasonPhrase);
            report.state = State.ACCOUNTED;
            reporter.getStatusHandler().setHandled(report.job, report.creationTime);
        } catch (RuntimeException e) {
            LOGGER.warning("Sending report " + report.job + " to accounting failed: " + e.getMessage());
            e.printStackTrace();
            report.state = State.NOT_YET_ACCOUNTED;
            reporter.getStatusHandler().setFailed(report.job, report.creationTime);
            reporter.getTimer().schedule(report, 60, TimeUnit.SECONDS);
        }
    }
}
