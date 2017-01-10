package com.bc.calvalus.generator;

import com.bc.calvalus.generator.writer.JobDetailWriter;

import java.util.concurrent.Executors;

import static java.util.concurrent.TimeUnit.SECONDS;

public class Launcher implements Runnable {
    private final int initialDelay = 0;
    private int timeIntervalInMinutes;
    private String urlPath;

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
        writeJobDetail.start();
    }

    public void start() {
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(this, initialDelay, timeIntervalInMinutes, SECONDS);
    }
}
