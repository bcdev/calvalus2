package com.bc.calvalus.production.local;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.JobIdFormat;
import com.bc.calvalus.processing.ProcessingService;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

/**
 * A processing system that is implemented locally (e.g. using BEAM).
 */
class LocalProcessingService implements ProcessingService<String> {
    static long jobNum = System.nanoTime();

    private final Map<String, Job> jobs;
    private final Map<String, ProcessStatus> jobStatuses;

    public LocalProcessingService() {
        jobs = new HashMap<String, Job>();
        jobStatuses = new HashMap<String, ProcessStatus>();
    }

    @Override
    public JobIdFormat<String> getJobIdFormat() {
        return JobIdFormat.STRING;
    }

    @Override
    public String getDataInputPath(String inputPath) {
        return new File(System.getProperty("user.home"), ".calvalus/test-input-data").getPath();
    }

    @Override
    public String getDataOutputPath(String outputPath) {
        return new File(System.getProperty("user.home"), ".calvalus/test-output-data").getPath();
    }

    @Override
    public String getSoftwarePath() {
        return new File(System.getProperty("user.home"), ".calvalus/test-software").getPath();
    }

    @Override
    public String[] listFilePaths(String dirPath) throws IOException {
        return new File(dirPath).list();
    }

    @Override
    public InputStream open(String path) throws IOException {
        return new FileInputStream(path);
    }

    @Override
    public void updateStatuses() throws IOException {
        Set<Map.Entry<String, Job>> entries = jobs.entrySet();
        synchronized (jobStatuses) {
            for (Map.Entry<String, Job> entry : entries) {
                String id = entry.getKey();
                Job job = entry.getValue();
                ProcessStatus processStatus;
                if (job.isKilled()) {
                    processStatus = new ProcessStatus(ProcessState.CANCELLED);
                } else if (job.getProgress() >= 1.0f) {
                    processStatus = new ProcessStatus(ProcessState.COMPLETED);
                } else {
                    processStatus = new ProcessStatus(ProcessState.RUNNING, job.getProgress());
                }
                jobStatuses.put(id, processStatus);
            }
        }
    }

    @Override
    public ProcessStatus getJobStatus(String jobId) {
        synchronized (jobStatuses) {
            return jobStatuses.get(jobId);
        }
    }

    @Override
    public synchronized boolean killJob(String jobId) throws IOException {
        Job job = jobs.get(jobId);
        if (job != null && !job.isKilled()) {
            job.kill();
            return true;
        }
        return false;
    }

    @Override
    public void close() throws IOException {
    }

    public synchronized String submitJob() {
        Job job = new Job(createJobId(), 1000 * (10 + (int) (50 * Math.random())));
        job.submit();
        jobs.put(job.getId(), job);
        return job.getId();
    }

    private static String createJobId() {
        return "job_" + Long.toHexString(++jobNum);
    }

    public class Job extends TimerTask {

        private final String id;
        private final int durationMillis;
        private Timer timer;
        private long startTime;
        private float progress;
        private boolean killed;

        public Job(String id, int durationMillis) {
            this.id = id;
            this.durationMillis = durationMillis;
        }

        public void submit() {
            this.timer = new Timer();
            this.startTime = System.currentTimeMillis();
            timer.scheduleAtFixedRate(this, 0, 100);
        }

        public String getId() {
            return id;
        }

        public float getProgress() {
            return progress;
        }

        public void kill() {
            killed = true;
            timer.cancel();
        }

        public boolean isKilled() {
            return killed;
        }

        @Override
        public void run() {
            if (killed) {
                return;
            }
            float progress = (float) (System.currentTimeMillis() - startTime) / (float) durationMillis;
            if (progress >= 1.0f) {
                this.progress = 1.0f;
                timer.cancel();
            } else {
                this.progress = progress;
            }
            // System.out.println("progress = " + progress);
        }

    }
}
