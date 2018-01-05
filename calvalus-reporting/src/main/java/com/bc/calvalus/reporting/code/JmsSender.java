package com.bc.calvalus.reporting.code;

import javax.jms.JMSException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author hans
 */
public class JmsSender {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Invalid arguments.");
            System.exit(-1);
        }
        String messageConsumerUrl = args[0];
        String queueName = args[0];
        String accountingFilePath = args[2];
        JmsClient jmsClient = null;
        try {
            jmsClient = new JmsClient(messageConsumerUrl, queueName);
            Path filePath = Paths.get(accountingFilePath);
            String jsonContent = new String(Files.readAllBytes(filePath));
            System.out.println("messageConsumerUrl: " + messageConsumerUrl);
            System.out.println("queueName: " + queueName);
            System.out.println("accountingFilePath: " + accountingFilePath);
            jmsClient.sendMessage(jsonContent);
            System.out.println("========content-sent========");
            System.out.println(jsonContent);
            System.out.println("========content-sent========");
            System.exit(0);
        } catch (JMSException | URISyntaxException | IOException exception) {
            exception.printStackTrace();
            System.exit(-1);
        } finally {
            try {
                if(jmsClient != null){
                    jmsClient.closeConnection();
                }
            } catch (JMSException ignored) {
            }
        }
    }
}
