package com.bc.calvalus.portal.server;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
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
    private final File outputFile;
    private final long startTime;
    private final long duration;
    private Timer timer;
    private float progress;
    private boolean cancelled;

    /**
     * Constructs a new dummy production.
     *
     * @param name     Some name.
     * @param duration The total time in ms to run.
     */
    public DummyProduction(String name, long duration, File outputFile) {
        this.id = Long.toHexString(idGen.nextLong());
        this.name = name;
        this.outputFile = outputFile;
        this.duration = duration;
        this.startTime = System.currentTimeMillis();
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getOutputPath() {
        return outputFile != null ? outputFile.getName() : null;
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
                progress = (float) (System.currentTimeMillis() - startTime) / (float) duration;
                if (progress >= 1.0f) {
                    progress = 1.0f;
                    if (timer != null) {
                        if (outputFile != null) {
                            try {
                                writeOutputFile();
                            } catch (Exception e) {
                                // shit
                            }
                        }
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

    private void writeOutputFile() throws Exception {
        if (!outputFile.exists()) {
            outputFile.getParentFile().mkdirs();
            FileOutputStream stream = new FileOutputStream(outputFile);
            byte[] buffer = new byte[1024 * 1024];
            try {
                for (int i = 0; i < 32; i++) {
                    Arrays.fill(buffer, (byte) i);
                    stream.write(buffer);
                }
            } finally {
                stream.close();
            }
        }
    }


}
