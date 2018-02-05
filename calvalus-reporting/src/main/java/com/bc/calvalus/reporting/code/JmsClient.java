package com.bc.calvalus.reporting.code;

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

/**
 * @author hans
 */
public class JmsClient {

    private final Session jmsSession;
    private final MessageProducer jmsProducer;
    private final Connection connection;

    public JmsClient(String messageConsumerUrl, String queueName) throws URISyntaxException, JMSException {
        URI uri = new URI(messageConsumerUrl);
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(uri);
        connection = activeMQConnectionFactory.createConnection();
        connection.start();
        jmsSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = jmsSession.createQueue(queueName);
        jmsProducer = jmsSession.createProducer(destination);
        jmsProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    }

    public void sendMessage(String messageJson) throws JMSException {
        TextMessage textMessage = jmsSession.createTextMessage(messageJson);
        jmsProducer.send(textMessage);
    }

    public void closeConnection() throws JMSException {
        if(connection != null){
            connection.close();
        }
    }

}
