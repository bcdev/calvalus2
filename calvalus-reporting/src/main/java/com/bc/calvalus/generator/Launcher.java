package com.bc.calvalus.generator;

import com.bc.calvalus.generator.writer.JobDetailWriter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.SECONDS;

public class Launcher implements Runnable {
    private int timeIntervalInMinutes;
    private String urlPath;
    public static transient boolean terminate = false;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public static Launcher builder() {
        return new Launcher();
    }

    public Launcher setTimeIntervalInMinutes(int inMinutes) {
        timeIntervalInMinutes = inMinutes;
        return this;
    }

    public Launcher setUrlPath(String urlPath) {
        this.urlPath = urlPath;
        return this;
    }

    @Override
    public void run() {
        JobDetailWriter writeJobDetail = new JobDetailWriter(urlPath);
        if (!terminate) {
            writeJobDetail.start();
        } else {
            stop();
        }
    }

    private void stop() {
        scheduledExecutorService.shutdownNow();
    }

    public void start() {
        int initialDelay = 0;
        scheduledExecutorService.scheduleWithFixedDelay(this, initialDelay, timeIntervalInMinutes, SECONDS);
    }
}
