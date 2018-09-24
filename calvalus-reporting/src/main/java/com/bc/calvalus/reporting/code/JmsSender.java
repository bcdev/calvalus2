package com.bc.calvalus.reporting.code;

import com.bc.calvalus.commons.util.PropertiesWrapper;

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

    private static final int MAX_SEND_COUNT = 10;

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Invalid arguments.");
            System.exit(-1);
        }
        System.out.println(args.length);
        for (String arg : args) {
            System.out.println("arg: " + arg);
        }
        String messageConsumerUrl = args[0];
        String queueName = args[1];
        String accountingFilePath = args[2];
        boolean bulkSend = false;
        if (args.length > 3 && "bulk".equalsIgnoreCase(args[3])) {
            bulkSend = true;
        }
        JmsClient jmsClient = null;
        try {
            PropertiesWrapper.loadConfigFile("code.properties");
            int maxSendCount = MAX_SEND_COUNT;
            try {
                int count = PropertiesWrapper.getInteger("jms.bulk.count");
                if (count > 0) {
                    maxSendCount = count;
                }
            } catch (NumberFormatException ignored) {
            }
            jmsClient = new JmsClient(messageConsumerUrl, queueName);
            Path filePath = Paths.get(accountingFilePath);
            String jsonContent = new String(Files.readAllBytes(filePath));
            System.out.println("messageConsumerUrl: " + messageConsumerUrl);
            System.out.println("queueName: " + queueName);
            System.out.println("accountingFilePath: " + accountingFilePath);
            int sendCount = 0;
            do {
                jmsClient.sendMessage(jsonContent);
                sendCount++;
                System.out.println("Sent message " + sendCount);
            } while (bulkSend && sendCount < maxSendCount);
            System.exit(0);
        } catch (JMSException | URISyntaxException | IOException exception) {
            exception.printStackTrace();
            System.exit(-1);
        } finally {
            try {
                if (jmsClient != null) {
                    jmsClient.closeConnection();
                }
            } catch (JMSException ignored) {
            }
        }
    }
}
