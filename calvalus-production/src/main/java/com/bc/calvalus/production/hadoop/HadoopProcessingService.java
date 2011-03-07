package com.bc.calvalus.production.hadoop;


import com.bc.calvalus.production.ProductionState;
import com.bc.calvalus.production.ProductionStatus;
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

public class HadoopProcessingService implements ProcessingService {
    private final JobClient jobClient;
    private final FileSystem fileSystem;
    private final Path dataArchiveRootPath;
    private final Path dataOutputRootPath;

    public HadoopProcessingService(JobClient jobClient) throws IOException {
        this.jobClient = jobClient;
        this.fileSystem = FileSystem.get(jobClient.getConf());
        // String fsName = jobClient.getConf().get("fs.default.name");
        this.dataArchiveRootPath = fileSystem.makeQualified(new Path("/calvalus/eodata"));
        this.dataOutputRootPath =  fileSystem.makeQualified(new Path("/calvalus/outputs"));
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
    public String getDataArchiveRootPath() {
        return dataArchiveRootPath.toString();
    }

    @Override
    public String getDataOutputRootPath() {
        return dataOutputRootPath.toString();
    }


    @Override
    public Map<Object, ProductionStatus> getJobStatusMap() throws IOException {
        JobStatus[] jobStatuses = jobClient.getAllJobs();
        HashMap<Object, ProductionStatus> jobStatusMap = new HashMap<Object, ProductionStatus>();
        for (JobStatus jobStatus : jobStatuses) {
            jobStatusMap.put(jobStatus.getJobID(), convertStatus(jobStatus));
        }
        return jobStatusMap;
    }

    @Override
    public boolean killJob(Object jobId) throws IOException {
        org.apache.hadoop.mapred.JobID oldJobId = org.apache.hadoop.mapred.JobID.downgrade((JobID) jobId);
        RunningJob runningJob = jobClient.getJob(oldJobId);
        if (runningJob != null) {
            runningJob.killJob();
            return true;
        }
        return false;
    }

    JobClient getJobClient() {
        return jobClient;
    }


    /**
     * Updates the status. This method is called periodically after a fixed delay period.
     *
     * @param jobStatus The hadoop job status. May be null, which is interpreted as the job is being done.
     */
    static ProductionStatus convertStatus(JobStatus jobStatus) {
        if (jobStatus != null) {
            float progress = (jobStatus.mapProgress() + jobStatus.reduceProgress()) / 2;
            if (jobStatus.getRunState() == JobStatus.FAILED) {
                return new ProductionStatus(ProductionState.ERROR, progress, "Hadoop job '" + jobStatus.getJobID() + "' failed");
            } else if (jobStatus.getRunState() == JobStatus.KILLED) {
                return new ProductionStatus(ProductionState.CANCELLED, progress);
            } else if (jobStatus.getRunState() == JobStatus.PREP) {
                return new ProductionStatus(ProductionState.WAITING, progress);
            } else if (jobStatus.getRunState() == JobStatus.RUNNING) {
                return new ProductionStatus(ProductionState.IN_PROGRESS, progress);
            } else if (jobStatus.getRunState() == JobStatus.SUCCEEDED) {
                return new ProductionStatus(ProductionState.COMPLETED, 1.0f);
            }
        }
        return ProductionStatus.UNKNOWN;
    }
}
