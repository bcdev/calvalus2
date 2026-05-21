package com.bc.calvalus.production.cli;

import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;

import java.io.IOException;
import java.util.Map;

public abstract class CalvalusHadoopStatusConverter {

    public static CalvalusHadoopStatusConverter create(CalvalusHadoopConnection hadoopConnection, String format) {
        switch (format) {
            case "csv":
                return new CalvalusHadoopCsvStatusConverter(hadoopConnection);
            case "json":
            default:
                return new CalvalusHadoopJsonStatusConverter(hadoopConnection);
        }
    }

    private final CalvalusHadoopConnection hadoopConnection;

    public CalvalusHadoopStatusConverter(CalvalusHadoopConnection hadoopConnection) {
        this.hadoopConnection = hadoopConnection;
    }

    /**
     * Look up job status by ID
     */

    public static JobStatus findById(String id, JobStatus[] jobs) {
        for (JobStatus status : jobs) {
            if (status.getJobID().toString().equals(id)) {
                return status;
            }
        }
        return null;
    }

    /**
     * Convert job status into Json string
     */

    public void accumulateJobStatus(String id, JobStatus job, StringBuilder accu) throws IOException {
        separateJobStatus(accu);
        double progress = job != null ? job.getMapProgress() * 0.9 + job.getReduceProgress() * 0.1 : 0.0;
        if (job == null) {
            accumulateJobStatus(id, "NOT_FOUND", progress, null, accu);
        } else if (job.getRunState() == JobStatus.SUCCEEDED) {
            accumulateJobStatus(id, "SUCCEEDED", progress, null, accu);
        } else if (job.getRunState() == JobStatus.FAILED) {
            accumulateJobStatus(id, "FAILED", progress, hadoopConnection.getDiagnostics(job.getJobID()), accu);
        } else if (job.getRunState() == JobStatus.KILLED) {
            accumulateJobStatus(id, "CANCELLED", progress, hadoopConnection.getDiagnostics(job.getJobID()), accu);
        } else if (job.getRunState() == JobStatus.RUNNING) {
            accumulateJobStatus(id, "RUNNING", progress, null, accu);
        } else if (job.getRunState() == JobStatus.PREP) {
            accumulateJobStatus(id, "WAITING", progress, null, accu);
        } else {
            throw new IllegalArgumentException("unknown status " + job.getRunState());
        }
    }

    public void accumulateJob(String id, RunningJob job, StringBuilder accu) throws IOException {
        separateJobStatus(accu);
        accu.append(job.getJobFile());
        //accumulateJobConfiguration(id, job.getConfiguration(), accu);
    }

    /**
     * Compose formatted string for status
     */
    public abstract void accumulateJobStatus(String id, String status, double progress, String message, StringBuilder accu);

    public abstract void accumulateJobConfiguration(String id, Iterable<Map.Entry<String, String>> configuration, StringBuilder accu);

    public abstract void initialiseJobStatus(StringBuilder accu);

    protected abstract void separateJobStatus(StringBuilder accu);

    public abstract void finaliseJobStatus(StringBuilder accu);
}