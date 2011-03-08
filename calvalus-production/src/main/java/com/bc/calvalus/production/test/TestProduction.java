package com.bc.calvalus.production.test;

import com.bc.calvalus.production.ProcessState;
import com.bc.calvalus.production.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionRequest;

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
    private final File outputFile;
    private Timer timer;

    /**
     * Constructs a new dummy production.
     *
     * @param name     Some name.
     * @param duration The total time in ms to run.
     * @param outputUrl The relative output URL
     * @param outputFile The path to the local file to be created
     * @param outputStaging true, if auto-staging enabled
     */
    public TestProduction(String name, long duration, String outputUrl, File outputFile, boolean outputStaging, ProductionRequest productionRequest) {
        super(Long.toHexString(idGen.nextLong()), name, System.getProperty("user.name"), outputStaging, new Object[0], productionRequest);
        this.outputFile = outputFile;
        this.duration = duration;
        this.startTime = System.currentTimeMillis();

        if (outputUrl != null) {
            setOutputUrl(outputUrl);
            if (outputStaging) {
                setStagingStatus(new ProcessStatus(ProcessState.WAITING));
            }
        }
    }


    public void start() {
        setProcessingStatus(new ProcessStatus(ProcessState.WAITING));
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                float progress = (float) (System.currentTimeMillis() - startTime) / (float) duration;
                if (progress >= 1.0f) {
                    setProcessingStatus(new ProcessStatus(ProcessState.COMPLETED, 1.0f));
                    if (timer != null) {
                        if (isOutputStaging() && getOutputUrl() != null) {
                            stageOutput();
                        }
                        stopTimer();
                    }
                } else {
                    setProcessingStatus(new ProcessStatus(ProcessState.IN_PROGRESS, progress));
                }
            }
        };
        timer = new Timer();
        timer.scheduleAtFixedRate(task, 0, 100);
    }

    public void cancel() {
        if (timer != null) {
            stopTimer();
            setProcessingStatus(new ProcessStatus(ProcessState.CANCELLED, 0.0f, "Cancelled"));
        }
    }

    private void stopTimer() {
        timer.cancel();
        timer = null;
    }

    void stageOutput() {
        try {
            setStagingStatus(new ProcessStatus(ProcessState.IN_PROGRESS, 0.0f));
            if (!outputFile.exists()) {
                File parentFile = outputFile.getParentFile();
                if (parentFile != null) {
                    parentFile.mkdirs();
                }
                FileOutputStream stream = new FileOutputStream(outputFile);
                byte[] buffer = new byte[1024 * 1024];
                try {
                    for (int i = 0; i < 32; i++) {
                        setStagingStatus(new ProcessStatus(ProcessState.IN_PROGRESS, i / 32f));
                        Arrays.fill(buffer, (byte) i);
                        stream.write(buffer);
                    }
                } finally {
                    stream.close();
                }
            }
            setStagingStatus(new ProcessStatus(ProcessState.COMPLETED, 1f));
        } catch (IOException e) {
            setStagingStatus(new ProcessStatus(ProcessState.ERROR, getStagingStatus().getProgress(), e.getMessage()));
        }
    }


}
