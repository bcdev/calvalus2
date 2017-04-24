package com.bc.calvalus.urban;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author muhammad.bc.
 */
public class Launcher implements Runnable {

    public static final int DELAY = 3000;
    private final SendWriteMessage sendWriteMessage;

    public Launcher() {
        sendWriteMessage = new SendWriteMessage();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleWithFixedDelay(this, 0, DELAY, SECONDS);
    }

    public static void main(String[] args) {
        new Launcher();
    }

    @Override
    public void run() {
        sendWriteMessage.start();
    }
}
