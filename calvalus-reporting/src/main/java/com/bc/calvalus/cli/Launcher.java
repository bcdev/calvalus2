package com.bc.calvalus.cli;

import com.bc.calvalus.generator.writer.WriteJobDetail;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.SECONDS;

public class Launcher implements Runnable {
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final int initialDelay = 0;
    private int timeIntervalInMinutes;
    private String urlPath;
    private ScheduledFuture<?> scheduledFuture;

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
//        if (scheduledFuture.isDone()) {
        WriteJobDetail writeJobDetail = new WriteJobDetail(urlPath);
        writeJobDetail.start();
//        }
    }

    public void start() {
        scheduledFuture = scheduler.scheduleWithFixedDelay(this, initialDelay, timeIntervalInMinutes, SECONDS);
    }
}
