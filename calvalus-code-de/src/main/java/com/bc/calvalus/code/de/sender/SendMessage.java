package com.bc.calvalus.code.de.sender;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;
import java.io.IOException;
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

    private ProcessedMessage[] processedMessages;
    private Session session;
    private MessageProducer producer;
    private static Logger logger = CalvalusLogger.getLogger();
    private Connection connection;

    public SendMessage(ProcessedMessage... processedMessages) {
        this.processedMessages = processedMessages;
        try {
            PropertiesWrapper.loadConfigFile("conf/code-de.properties");
            initMessageProvider();
        } catch (JMSException e) {
            logger.log(Level.SEVERE, e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
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


    private void initMessageProvider() throws JMSException {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
        connection = activeMQConnectionFactory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        String queueName = PropertiesWrapper.get("queue.name");
        Destination destination = session.createQueue(queueName);
        producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
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
