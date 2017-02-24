package com.bc.calvalus.code.de;

import com.bc.calvalus.code.de.reader.JobDetail;
import com.bc.calvalus.code.de.reader.ReadJobDetail;
import com.bc.wps.utilities.PropertiesWrapper;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * @author muhammad.bc.
 */
public class Launcher implements Runnable {
    public Launcher() {
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleWithFixedDelay(this, 0, 5, MINUTES);

    }

    @Override
    public void run() {
        loadProperties();
        ReadJobDetail readJobDetail = new ReadJobDetail();
        List<JobDetail> jobDetail = readJobDetail.getJobDetail();
//        ProcessedMessage[] processedMessage = FactoryProcessedMessage.createProcessedMessage(jobDetail);
//        SendMessage sendMessage = new SendMessage(processedMessage);
//        sendMessage.send();
    }

    static void loadProperties() {
        try {
            PropertiesWrapper.loadConfigFile("conf/code-de.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Launcher launcher = new Launcher();
    }
}
