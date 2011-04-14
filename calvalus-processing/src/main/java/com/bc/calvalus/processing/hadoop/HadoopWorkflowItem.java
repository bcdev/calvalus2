/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.hadoop;

import com.bc.calvalus.commons.AbstractWorkflowItem;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static com.bc.calvalus.processing.hadoop.HadoopProcessingService.*;

/**
 * A workflow item that corresponds to a single Hadoop job.
 */
public abstract class HadoopWorkflowItem extends AbstractWorkflowItem {

    private final HadoopProcessingService processingService;
    private JobID jobId;

    public HadoopWorkflowItem(HadoopProcessingService processingService) {
        this.processingService = processingService;
    }

    public HadoopProcessingService getProcessingService() {
        return processingService;
    }

    protected JobID getJobId() {
        return jobId;
    }

    protected void setJobId(JobID jobId) {
        this.jobId = jobId;
    }

    @Override
    public void kill() throws WorkflowException {
        try {
            processingService.killJob(getJobId());
        } catch (IOException e) {
            throw new WorkflowException("Failed to kill Hadoop job: " + e.getMessage(), e);
        }
    }

    @Override
    public void updateStatus() {
        if (jobId != null) {
            ProcessStatus jobStatus = processingService.getJobStatus(jobId);
            if (jobStatus != null) {
                setStatus(jobStatus);
            }
        }
    }

    @Override
    public Object[] getJobIds() {
        return jobId != null ? new Object[]{getJobId()} : new Object[0];
    }

    @Override
    public void submit() throws WorkflowException {
        try {
            Job job = createJob();
            JobID jobId = submitJob(job);
            setJobId(jobId);
        } catch (IOException e) {
            throw new WorkflowException("Failed to submit Hadoop job: " + e.getMessage(), e);
        }
    }

    protected abstract Job createJob() throws IOException;

    protected JobID submitJob(Job job) throws IOException {
        Configuration configuration = job.getConfiguration();
        // Add Calvalus modules to classpath of Hadoop jobs
        addBundleToClassPath(CALVALUS_BUNDLE, configuration);
        addBundleToClassPath(BEAM_BUNDLE, configuration);
        JobConf jobConf;
        if (configuration instanceof JobConf) {
            jobConf = (JobConf) configuration;
        } else {
            jobConf = new JobConf(configuration);
        }
        jobConf.setUseNewMapper(true);
        jobConf.setUseNewReducer(true);
        RunningJob runningJob = processingService.getJobClient().submitJob(jobConf);
        return runningJob.getID();
    }

    protected static void setAndClearOutputDir(Job job, String outputDir) throws IOException {
        final Path outputPath = new Path(outputDir);
        final FileSystem fileSystem = outputPath.getFileSystem(job.getConfiguration());
        fileSystem.delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);
    }


}
