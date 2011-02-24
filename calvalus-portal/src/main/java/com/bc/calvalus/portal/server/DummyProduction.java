package com.bc.calvalus.portal.server;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Dummy production that simulates progress over a given amount of time.
 *
 * @author Norman
 */
class DummyProduction {
    private static final Random idGen = new Random();
    private final String id;
    private final String name;
    private final long startTime;
    private final long totalTime;
    private Timer timer;
    private float progress;
    private boolean cancelled;

    /**
     * Constructs a new dummy production.
     *
     * @param name      Some name.
     * @param totalTime The total time in ms to run.
     */
    public DummyProduction(String name, long totalTime) {
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

    public float getProgress() {
        return progress;
    }

    public boolean isDone() {
        return timer == null;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public void start() {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                progress = (float) (System.currentTimeMillis() - startTime) / (float) totalTime;
                if (progress >= 1.0f) {
                    progress = 1.0f;
                    if (timer != null) {
                        stopTimer();
                    }
                }
            }
        };
        timer = new Timer();
        timer.scheduleAtFixedRate(task, 0, 100);
        cancelled = false;
    }

    public void cancel() {
        if (timer != null) {
            stopTimer();
            cancelled = true;
        }
    }

    private void stopTimer() {
        timer.cancel();
        timer = null;
    }
}
