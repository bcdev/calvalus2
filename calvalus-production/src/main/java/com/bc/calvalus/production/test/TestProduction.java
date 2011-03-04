package com.bc.calvalus.production.test;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionState;
import com.bc.calvalus.production.ProductionStatus;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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
    private final boolean autoStage;

    /**
     * Constructs a new dummy production.
     *
     * @param name     Some name.
     * @param duration The total time in ms to run.
     */
    public TestProduction(String name, long duration, String outputFileName, boolean autoStage) {
        super(Long.toHexString(idGen.nextLong()), name);
        this.autoStage = autoStage;
        this.duration = duration;
        this.startTime = System.currentTimeMillis();

        if (outputFileName != null) {
            File downloadDir = new File(System.getProperty("user.home"), ".calvalus/test");
            setOutputUrl(new File(downloadDir, outputFileName).getPath());
            if (autoStage) {
                setStagingStatus(new ProductionStatus(ProductionState.WAITING));
            }
        }
    }


    public void start() {
        setProcessingStatus(new ProductionStatus(ProductionState.WAITING));
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                float progress = (float) (System.currentTimeMillis() - startTime) / (float) duration;
                if (progress >= 1.0f) {
                    setProcessingStatus(new ProductionStatus(ProductionState.COMPLETED, 1.0f));
                    if (timer != null) {
                        if (autoStage && getOutputUrl() != null) {
                            writeOutputFile();
                        }
                        stopTimer();
                    }
                } else {
                    setProcessingStatus(new ProductionStatus(ProductionState.IN_PROGRESS, progress));
                }
            }
        };
        timer = new Timer();
        timer.scheduleAtFixedRate(task, 0, 100);
    }

    public void cancel() {
        if (timer != null) {
            stopTimer();
            setProcessingStatus(new ProductionStatus(ProductionState.CANCELLED, 0.0f, "Cancelled"));
        }
    }

    private void stopTimer() {
        timer.cancel();
        timer = null;
    }

    private void writeOutputFile() {
        try {
            setStagingStatus(new ProductionStatus(ProductionState.IN_PROGRESS, 0.0f));
            File outputFile = new File(getOutputUrl());
            if (!outputFile.exists()) {
                File parentFile = outputFile.getParentFile();
                if (parentFile != null) {
                    parentFile.mkdirs();
                }
                FileOutputStream stream = new FileOutputStream(outputFile);
                byte[] buffer = new byte[1024 * 1024];
                try {
                    for (int i = 0; i < 32; i++) {
                        setStagingStatus(new ProductionStatus(ProductionState.IN_PROGRESS, i / 32f));
                        Arrays.fill(buffer, (byte) i);
                        stream.write(buffer);
                    }
                } finally {
                    stream.close();
                }
            }
            setStagingStatus(new ProductionStatus(ProductionState.COMPLETED, 1f));
        } catch (IOException e) {
            setStagingStatus(new ProductionStatus(ProductionState.ERROR, getStagingStatus().getProgress(), e.getMessage()));
        }
    }


}
