package com.bc.calvalus.production.test;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionState;
import com.bc.calvalus.production.ProductionStatus;

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
class TestProduction extends Production {
    private static final Random idGen = new Random();
    private final long startTime;
    private final long duration;
    private Timer timer;

    /**
     * Constructs a new dummy production.
     *
     * @param name     Some name.
     * @param duration The total time in ms to run.
     */
    public TestProduction(String name, long duration, String outputPath) {
        super(Long.toHexString(idGen.nextLong()), name, outputPath);
        this.duration = duration;
        this.startTime = System.currentTimeMillis();
    }


    public void start() {
        setStatus(new ProductionStatus(ProductionState.WAITING));
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                float progress = (float) (System.currentTimeMillis() - startTime) / (float) duration;
                if (progress >= 1.0f) {
                    setStatus(new ProductionStatus(ProductionState.COMPLETED, 1.0f));
                    if (timer != null) {
                        if (getOutputPath() != null) {
                            try {
                                writeOutputFile();
                            } catch (Exception e) {
                                // shit
                            }
                        }
                        stopTimer();
                    }
                } else {
                    setStatus(new ProductionStatus(ProductionState.IN_PROGRESS, progress));
                }
            }
        };
        timer = new Timer();
        timer.scheduleAtFixedRate(task, 0, 100);
    }

    public void cancel() {
        if (timer != null) {
            stopTimer();
            setStatus(new ProductionStatus(ProductionState.CANCELLED, "Cancelled", 0.0f));
        }
    }

    private void stopTimer() {
        timer.cancel();
        timer = null;
    }

    private void writeOutputFile() throws Exception {
        File outputFile = new File(getOutputPath());
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
