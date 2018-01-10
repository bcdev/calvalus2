package com.bc.calvalus.reporting.code.sender;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.code.CodeReport;
import com.google.gson.Gson;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author muhammad.bc.
 */
public class SendMessage {

    private static final String MESSAGE_CONSUMER_URL = "message.consumer.url";
    private static final String QUEUE_NAME = "queue.name";
    private static final Logger logger = CalvalusLogger.getLogger();
    private final CodeReport[] codeReports;
    private Session session;
    private MessageProducer producer;
    private Connection connection;

    public SendMessage(CodeReport... codeReports) {
        this.codeReports = codeReports;
        initMessageProvider();
    }

    public Boolean send() {
        boolean isSend = false;
        try {
            send(session, producer, codeReports);
            close();
            isSend = true;
        } catch (JMSException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
        return isSend;
    }


    private void initMessageProvider() {
        try {
            URI uri = new URI(System.getProperty(MESSAGE_CONSUMER_URL));
            ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(uri);
            connection = activeMQConnectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = System.getProperty(QUEUE_NAME);
            Destination destination = session.createQueue(queueName);
            producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        } catch (JMSException e) {
            logger.log(Level.WARNING, e.getMessage());
        } catch (URISyntaxException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    private void close() throws JMSException {
        session.close();
        producer.close();
        connection.close();
    }

    private void send(Session session, MessageProducer producer, CodeReport[] codeReports) throws JMSException {
        for (CodeReport codeReport : codeReports) {
            Gson gson = new Gson();
            String msg = gson.toJson(codeReport, CodeReport.class);
            TextMessage textMessage = session.createTextMessage(msg);
            producer.send(textMessage);
        }
    }

}
