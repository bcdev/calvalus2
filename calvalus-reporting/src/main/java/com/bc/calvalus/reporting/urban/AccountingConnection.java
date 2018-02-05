package com.bc.calvalus.reporting.urban;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.common.Report;
import com.bc.calvalus.reporting.common.State;
import com.bc.calvalus.reporting.urban.account.Account;
import com.bc.calvalus.reporting.urban.account.Any;
import com.bc.calvalus.reporting.urban.account.Compound;
import com.bc.calvalus.reporting.urban.account.Message;
import com.bc.calvalus.reporting.urban.account.Quantity;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.jetbrains.annotations.NotNull;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    private static final Logger LOGGER = CalvalusLogger.getLogger();
    private final UrbanTepReporting reporter;
    private Client urbantepWebClient = null;
    private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    static {
        TIME_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    AccountingConnection(UrbanTepReporting reporter) {
        this.reporter = reporter;
    }

    void send(Report report) {
        Message message = createMessage(report);
        String messageJson = message.toJson();
        Path reportsDirPath = Paths.get(reporter.getConfig().getProperty("reporting.urbantep.reportsdir"));
        if (!Files.exists(reportsDirPath)) {
            try {
                Files.createDirectory(reportsDirPath);
            } catch (IOException e) {
                LOGGER.warning("unable to create reporting directory '" + reportsDirPath + "'");
                report.state = State.NOT_YET_ACCOUNTED;
                reporter.getStatusHandler().setFailed(report.job, report.creationTime);
                reporter.getExecutorService().schedule(report, 60, TimeUnit.SECONDS);
                return;
            }
        }
        File file = new File(reportsDirPath.toFile(), report.job);
        try (FileWriter fileWriter = new FileWriter(file)) {
            fileWriter.append(messageJson).append('\n');
        } catch (IOException e) {
            LOGGER.warning("Writing report " + report.job + " to file failed: " + e.getMessage());
            report.state = State.NOT_YET_ACCOUNTED;
            reporter.getStatusHandler().setFailed(report.job, report.creationTime);
            reporter.getExecutorService().schedule(report, 60, TimeUnit.SECONDS);
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
                createWebClient();
            }
            String reasonPhrase = sendMessage(messageJson);
            LOGGER.info("report " + report.job + " sent to Urban TEP accounting: " + reasonPhrase);
            report.state = State.ACCOUNTED;
            reporter.getStatusHandler().setHandled(report.job, report.creationTime);
        } catch (RuntimeException e) {
            LOGGER.warning("Sending report " + report.job + " to accounting failed: " + e.getMessage());
            e.printStackTrace();
            report.state = State.NOT_YET_ACCOUNTED;
            reporter.getStatusHandler().setFailed(report.job, report.creationTime);
            reporter.getExecutorService().schedule(report, 60, TimeUnit.SECONDS);
        }
    }

    private void createWebClient() {
        urbantepWebClient = ClientBuilder.newClient();
        urbantepWebClient.register(HttpAuthenticationFeature.basicBuilder()
                                           .nonPreemptive().credentials(reporter.getConfig().getProperty("reporting.urbantep.user"),
                                                                        reporter.getConfig().getProperty("reporting.urbantep.password")).build());
    }

    private String sendMessage(String messageJson) {
        WebTarget target = urbantepWebClient.target(reporter.getConfig().getProperty("reporting.urbantep.url"));
        Entity<String> jsonEntity = Entity.json(messageJson);
        Invocation.Builder request = target.request(MediaType.TEXT_PLAIN_TYPE);
        Response response = request.post(jsonEntity);
        return response.getStatusInfo().getReasonPhrase();
    }

    @NotNull
    private Message createMessage(Report report) {
        Account account = new Account(reporter.getConfig().getProperty("reporting.urbantep.subsystem"),
                                      report.usageStatistic.getUser().replace("tep_", ""),
                                      report.usageStatistic.getRemoteRef());
        Compound compound = new Compound(report.requestId,
                                         report.usageStatistic.getJobName(),
                                         report.usageStatistic.getProcessType(),
                                         new Any(report.uri));
        List<Quantity> quantityList = Arrays.asList(
                new Quantity("CPU_MILLISECONDS", report.usageStatistic.getCpuMilliseconds()),
                new Quantity("PHYSICAL_MEMORY_BYTES", report.usageStatistic.getCpuMilliseconds() == 0 ? (report.usageStatistic.getMbMillisMapTotal() + report.usageStatistic.getMbMillisReduceTotal()) * 1024 * 1024 : (report.usageStatistic.getMbMillisMapTotal() + report.usageStatistic.getMbMillisReduceTotal()) / report.usageStatistic.getCpuMilliseconds() * 1024 * 1024),
                new Quantity("BYTE_READ", report.usageStatistic.getHdfsBytesRead()),
                new Quantity("BYTE_WRITTEN", report.usageStatistic.getHdfsBytesWritten()),
                new Quantity("PROC_INSTANCE", (long) report.usageStatistic.getMapsCompleted() + report.usageStatistic.getReducesCompleted()),
                new Quantity("NUM_REQ", 1));
        return new Message(report.usageStatistic.getJobId(),
                                      account,
                                      compound,
                                      quantityList,
                                      reporter.getConfig().getProperty("reporting.urbantep.origin"),
                                      TIME_FORMAT.format(new Date(report.usageStatistic.getFinishTime())),
                           "SUCCEEDED".equals(report.usageStatistic.getState()) ? "NOMINAL" : "DEGRADED");
    }
}
