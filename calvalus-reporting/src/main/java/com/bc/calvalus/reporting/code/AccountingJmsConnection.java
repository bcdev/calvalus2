package com.bc.calvalus.reporting.code;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.code.sender.ProcessedMessage;
import com.bc.calvalus.reporting.common.Report;
import com.bc.calvalus.reporting.common.State;
import org.apache.activemq.ActiveMQConnectionFactory;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
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
        ProcessedMessage processedMessage = new ProcessedMessage(report.usageStatistics);
        String messageJson = processedMessage.toJson();
        LOGGER.info("sending report " + report.usageStatistics.getJobId());
        Path reportsDirPath = Paths.get(reporter.getConfig().getProperty("reporting.code.reportsdir"));
        if (!Files.exists(reportsDirPath)) {
            try {
                Files.createDirectory(reportsDirPath);
            } catch (IOException e) {
                LOGGER.warning("unable to create reporting directory '" + reportsDirPath + "'");
                report.state = State.NOT_YET_ACCOUNTED;
                reporter.getStatusHandler().setFailed(report.job, report.creationTime);
                reporter.getTimer().schedule(report, 60, TimeUnit.SECONDS);
                return;
            }
        }
        File file = new File(reportsDirPath.toFile(), String.format("account-message-%s.json", report.job));
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
        if (Boolean.parseBoolean(reporter.getConfig().getProperty("reporting.code.nosending", "false"))) {
            LOGGER.warning("skip sending report " + report.job + " - reporting.code.nosending=true");
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
}