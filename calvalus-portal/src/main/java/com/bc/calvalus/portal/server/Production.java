package com.bc.calvalus.portal.server;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Dummy production that simulates progress over a given amount of time.
 */
class Production {
    private static final Random idGen = new Random();
    private final String id;
    private final String name;
    private final long startTime;
    private final long totalTime;
    private Timer timer;
    private double progress;

    /**
     * Constructs a new dummy production.
     *
     * @param name  Some name.
     * @param totalTime The total time in ms to run.
     */
    public Production(String name, long totalTime) {
        this.id = Long.toHexString(idGen.nextLong());
        this.name = name;
        this.totalTime = totalTime;
        this.startTime = System.currentTimeMillis();
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public double getProgress() {
        return progress;
    }

    public boolean isDone() {
        return timer == null;
    }

    public void start() {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                progress = (double) (System.currentTimeMillis() - startTime) / (double) totalTime;
                if (progress >= 1.0) {
                    timer.cancel();
                    timer = null;
                    progress = 1.0;
                }
            }
        };
        timer = new Timer();
        timer.scheduleAtFixedRate(task, 0, 100);
    }
}
