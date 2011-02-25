package com.bc.calvalus.portal.server.hadoop;

import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.JobID;

/**
 * A Hadoop workflow.
 *
 * @author Norman
 */
class HadoopProduction {
   public static enum Request {
       CANCEL,
       DELETE,
       RESTART,
   }

    private final String id;
    private final String name;
    private final String outputPath;
    private final JobID jobId;
    private JobStatus jobStatus;
    private Request request;

    public HadoopProduction(String id, String name, String outputPath, JobID jobId) {
        this.id = id;
        this.name = name;
        this.outputPath = outputPath;
        this.jobId = jobId;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public JobID getJobId() {
        return jobId;
    }

    public JobStatus getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
    }

    public Request getRequest() {
        return request;
    }

    public void setRequest(Request request) {
        this.request = request;
    }
}
