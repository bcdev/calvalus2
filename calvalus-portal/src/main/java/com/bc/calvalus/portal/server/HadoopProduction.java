package com.bc.calvalus.portal.server;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

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
    private final Job job;
    private JobStatus jobStatus;
    private Request request;

    public HadoopProduction(String id, String name, String outputPath, Job job) {
        this.id = id;
        this.name = name;
        this.outputPath = outputPath;
        this.job = job;
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

    public Job getJob() {
        return job;
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
