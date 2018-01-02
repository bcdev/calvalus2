package com.bc.calvalus.reporting.code;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.common.Report;
import com.bc.calvalus.reporting.common.State;
import com.bc.calvalus.reporting.urban.account.Account;
import com.bc.calvalus.reporting.urban.account.Any;
import com.bc.calvalus.reporting.urban.account.Compound;
import com.bc.calvalus.reporting.urban.account.Message;
import com.bc.calvalus.reporting.urban.account.Quantity;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.jetbrains.annotations.NotNull;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Hans Permana
 */
public class AccountingJmsConnection {

    private static final Logger LOGGER = CalvalusLogger.getLogger();
    private final CodeReporting reporter;
    private Session jmsSession;
    private MessageProducer jmsProducer = null;
    private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    static {
        TIME_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    AccountingJmsConnection(CodeReporting reporter) {
        this.reporter = reporter;
    }

    void send(Report report) {
        Message message = createMessage(report);
        String messageJson = message.toJson();
        File file = new File(reporter.getConfig().getProperty("reporting.urbantep.reportsdir"),
                             String.format("account-message-%s.json", report.job));
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
            if (jmsProducer == null) {
                createJmsProducer();
            }
            sendMessage(messageJson);
            LOGGER.info("report " + report.job + " sent to CODE accounting.");
            report.state = State.ACCOUNTED;
            reporter.getStatusHandler().setHandled(report.job, report.creationTime);
        } catch (JMSException e) {
            LOGGER.log(Level.SEVERE, "Unable to initialize JMS Producer", e);
        } catch (URISyntaxException e) {
            LOGGER.log(Level.SEVERE, "Invalid ActiveMQ URI", e);
        } catch (RuntimeException e) {
            LOGGER.warning("Sending report " + report.job + " to accounting failed: " + e.getMessage());
            e.printStackTrace();
            report.state = State.NOT_YET_ACCOUNTED;
            reporter.getStatusHandler().setFailed(report.job, report.creationTime);
            reporter.getTimer().schedule(report, 60, TimeUnit.SECONDS);
        }
    }

    private void createJmsProducer() throws URISyntaxException, JMSException {
        URI uri = new URI(reporter.getConfig().getProperty("message.consumer.url"));
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(uri);
        Connection connection = activeMQConnectionFactory.createConnection();
        connection.start();
        jmsSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = jmsSession.createQueue(reporter.getConfig().getProperty("queue.name"));
        jmsProducer = jmsSession.createProducer(destination);
        jmsProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    }

    private void sendMessage(String messageJson) throws JMSException {
        TextMessage textMessage = jmsSession.createTextMessage(messageJson);
        jmsProducer.send(textMessage);
    }

    @NotNull
    private Message createMessage(Report report) {
        Account account = new Account(reporter.getConfig().getProperty("reporting.urbantep.subsystem"),
                                      report.usageStatistics.getUser().replace("tep_", ""),
                                      report.usageStatistics.getRemoteRef());
        Compound compound = new Compound(report.requestId,
                                         report.usageStatistics.getJobName(),
                                         report.usageStatistics.getProcessType(),
                                         new Any(report.uri));
        List<Quantity> quantityList = Arrays.asList(
                    new Quantity("CPU_MILLISECONDS", report.usageStatistics.getCpuMilliseconds()),
                    new Quantity("PHYSICAL_MEMORY_BYTES",
                                 report.usageStatistics.getCpuMilliseconds() == 0 ? (report.usageStatistics.getMbMillisMapTotal() + report.usageStatistics.getMbMillisReduceTotal()) * 1024 * 1024 : (report.usageStatistics.getMbMillisMapTotal() + report.usageStatistics.getMbMillisReduceTotal()) / report.usageStatistics.getCpuMilliseconds() * 1024 * 1024),
                    new Quantity("BYTE_READ", report.usageStatistics.getHdfsBytesRead()),
                    new Quantity("BYTE_WRITTEN", report.usageStatistics.getHdfsBytesWritten()),
                    new Quantity("PROC_INSTANCE",
                                 (long) report.usageStatistics.getMapsCompleted() + report.usageStatistics.getReducesCompleted()),
                    new Quantity("NUM_REQ", 1));
        return new Message(report.usageStatistics.getJobId(),
                           account,
                           compound,
                           quantityList,
                           reporter.getConfig().getProperty("reporting.urbantep.origin"),
                           TIME_FORMAT.format(new Date(report.usageStatistics.getFinishTime())),
                           "SUCCEEDED".equals(report.usageStatistics.getState()) ? "NOMINAL" : "DEGRADED");
    }
}
