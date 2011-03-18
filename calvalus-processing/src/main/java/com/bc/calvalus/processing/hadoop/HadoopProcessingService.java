package com.bc.calvalus.processing.hadoop;


import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.JobIdFormat;
import com.bc.calvalus.processing.ProcessingService;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.JobID;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HadoopProcessingService implements ProcessingService<JobID> {
    private final JobClient jobClient;
    private final FileSystem fileSystem;
    private final Path dataInputPath;
    private final Path dataOutputPath;
    private final Map<JobID, ProcessStatus> jobStatusMap;

    public HadoopProcessingService(JobClient jobClient) throws IOException {
        this.jobClient = jobClient;
        this.fileSystem = FileSystem.get(jobClient.getConf());
        // String fsName = jobClient.getConf().get("fs.default.name");
        this.dataInputPath = fileSystem.makeQualified(new Path("/calvalus/eodata"));
        this.dataOutputPath =  fileSystem.makeQualified(new Path("/calvalus/outputs"));
        jobStatusMap = new HashMap<JobID, ProcessStatus>();
    }

    public JobClient getJobClient() {
        return jobClient;
    }

    @Override
    public JobIdFormat<JobID> getJobIdFormat() {
        return new HadoopJobIdFormat();
    }

    @Override
    public String[] listFilePaths(String dirPath) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(dirPath));
        String[] paths = new String[fileStatuses.length];
        for (int i = 0; i < fileStatuses.length; i++) {
            paths[i] = fileStatuses[i].getPath().toString();
        }
        return paths;
    }

    @Override
    public String getDataInputPath() {
        return dataInputPath.toString();
    }

    @Override
    public String getDataOutputPath() {
        return dataOutputPath.toString();
    }

    @Override
    public void updateStatuses() throws IOException {
        JobStatus[] jobStatuses = jobClient.getAllJobs();
        synchronized (jobStatusMap) {
            jobStatusMap.clear();
            for (JobStatus jobStatus : jobStatuses) {
                jobStatusMap.put(jobStatus.getJobID(), convertStatus(jobStatus));
            }
        }
    }

    @Override
    public ProcessStatus getJobStatus(JobID jobID) {
        synchronized (jobStatusMap) {
            return jobStatusMap.get(jobID);
        }
    }


    @Override
    public boolean killJob(JobID jobId) throws IOException {
        org.apache.hadoop.mapred.JobID oldJobId = org.apache.hadoop.mapred.JobID.downgrade(jobId);
        RunningJob runningJob = jobClient.getJob(oldJobId);
        if (runningJob != null) {
            runningJob.killJob();
            return true;
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        jobClient.close();
    }

    /**
     * Updates the status. This method is called periodically after a fixed delay period.
     *
     * @param jobStatus The hadoop job status. May be null, which is interpreted as the job is being done.
     */
    static ProcessStatus convertStatus(JobStatus jobStatus) {
        if (jobStatus != null) {
            float progress = (jobStatus.mapProgress() + jobStatus.reduceProgress()) / 2;
            if (jobStatus.getRunState() == JobStatus.FAILED) {
                return new ProcessStatus(ProcessState.ERROR, progress, "Hadoop job '" + jobStatus.getJobID() + "' failed");
            } else if (jobStatus.getRunState() == JobStatus.KILLED) {
                return new ProcessStatus(ProcessState.CANCELLED, progress);
            } else if (jobStatus.getRunState() == JobStatus.PREP) {
                return new ProcessStatus(ProcessState.SCHEDULED, progress);
            } else if (jobStatus.getRunState() == JobStatus.RUNNING) {
                return new ProcessStatus(ProcessState.RUNNING, progress);
            } else if (jobStatus.getRunState() == JobStatus.SUCCEEDED) {
                return new ProcessStatus(ProcessState.COMPLETED, 1.0f);
            }
        }
        return ProcessStatus.UNKNOWN;
    }
}
