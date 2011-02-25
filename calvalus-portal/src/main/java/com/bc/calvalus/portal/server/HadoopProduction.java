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

    private final String id;
    private final String name;
    private final Job job;
    private JobStatus jobStatus;

    public HadoopProduction(String id, String name, Job job) {
        this.id = id;
        this.name = name;
        this.job = job;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
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

}
