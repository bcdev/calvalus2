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
            System.err.println("Invalid arguments.");
            return;
        }
        String messageConsumerUrl = args[0];
        String queueName = args[0];
        String accountingFilePath = args[2];
        try {
            JmsClient jmsClient = new JmsClient(messageConsumerUrl, queueName);
            Path filePath = Paths.get(accountingFilePath);
            String jsonContent = new String(Files.readAllBytes(filePath));
            System.out.println("messageConsumerUrl: " + messageConsumerUrl);
            System.out.println("queueName: " + queueName);
            System.out.println("accountingFilePath: " + accountingFilePath);
            System.out.println("========content-to-be-sent========");
            System.out.println(jsonContent);
            System.out.println("========content-to-be-sent========");
            jmsClient.sendMessage(jsonContent);
        } catch (JMSException | URISyntaxException | IOException exception) {
            exception.printStackTrace();
        }
    }
}
