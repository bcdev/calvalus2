package com.bc.calvalus.code.de.sender;

import com.bc.calvalus.commons.CalvalusLogger;
import com.google.gson.Gson;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * @author muhammad.bc.
 */
public class SendMessage {

    private static final String MESSAGE_CONSUMER_URL = "message.consumer.url";
    private static final String QUEUE_NAME = "queue.name";
    private static final Logger logger = CalvalusLogger.getLogger();
    private final ProcessedMessage[] processedMessages;
    private Session session;
    private MessageProducer producer;
    private Connection connection;

    public SendMessage(ProcessedMessage... processedMessages) {
        this.processedMessages = processedMessages;
        initMessageProvider();
    }

    public Boolean send() {
        boolean isSend = false;
        try {
            send(session, producer, processedMessages);
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

    private void send(Session session, MessageProducer producer, ProcessedMessage[] processedMessages) throws JMSException {
        for (ProcessedMessage processedMessage : processedMessages) {
            Gson gson = new Gson();
            String msg = gson.toJson(processedMessage, ProcessedMessage.class);
            TextMessage textMessage = session.createTextMessage(msg);
            producer.send(textMessage);
        }
    }

}
