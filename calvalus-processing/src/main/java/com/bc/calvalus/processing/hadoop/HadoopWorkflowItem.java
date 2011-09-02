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
import com.bc.calvalus.processing.JobConfigNames;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;

import java.io.IOException;

import static com.bc.calvalus.processing.hadoop.HadoopProcessingService.*;

/**
 * A workflow item that corresponds to a single Hadoop job.
 */
public abstract class HadoopWorkflowItem extends AbstractWorkflowItem {

    protected static final String NO_DEFAULT = "[[NO_DEFAULT]]";
    private final HadoopProcessingService processingService;
    private final String jobName;
    private final Configuration jobConfig;
    private JobID jobId;

    public HadoopWorkflowItem(HadoopProcessingService processingService,
                              String jobName, Configuration jobConfig) {
        this.processingService = processingService;
        this.jobName = jobName;
        this.jobConfig = jobConfig;
    }

    public final HadoopProcessingService getProcessingService() {
        return processingService;
    }

    public final String getJobName() {
        return jobName;
    }

    public final Configuration getJobConfig() {
        return jobConfig;
    }

    public final JobID getJobId() {
        return jobId;
    }

    protected final void setJobId(JobID jobId) {
        this.jobId = jobId;
    }

    @Override
    public final Object[] getJobIds() {
        return jobId != null ? new Object[]{getJobId()} : new Object[0];
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
    public void submit() throws WorkflowException {
        try {
            Job job = getProcessingService().createJob(jobName, jobConfig);
            configureJob(job);
            validateJob(job);
            JobID jobId = submitJob(job);
            setJobId(jobId);
        } catch (IOException e) {
            throw new WorkflowException("Failed to submit Hadoop job: " + e.getMessage(), e);
        }
    }

    protected void validateJob(Job job) throws WorkflowException {
        Configuration jobConfig = job.getConfiguration();
        String[][] configDefaults = getJobConfigDefaults();
        for (int i = 0; i < configDefaults.length; i++) {
            String[] configDefault = configDefaults[i];
            String propertyName = configDefault[0];
            String propertyDefault = configDefault[1];
            String propertyValue = jobConfig.get(propertyName);
            if (propertyValue == null) {
                if (NO_DEFAULT.equals(propertyDefault)) {
                    throw new WorkflowException("Missing value for job configuration property '" + propertyName + "'");
                }  else if (propertyDefault != null) {
                    jobConfig.set(propertyName, propertyDefault);
                }
            }
        }
    }

    protected abstract void configureJob(Job job) throws IOException;

    protected abstract String[][] getJobConfigDefaults();

    protected JobID submitJob(Job job) throws IOException {
        Configuration configuration = job.getConfiguration();
        // Add Calvalus modules to classpath of Hadoop jobs
        addBundleToClassPath(configuration.get(JobConfigNames.CALVALUS_CALVALUS_BUNDLE, DEFAULT_CALVALUS_BUNDLE), configuration);
        // Add BEAM modules to classpath of Hadoop jobs
        addBundleToClassPath(configuration.get(JobConfigNames.CALVALUS_BEAM_BUNDLE, DEFAULT_BEAM_BUNDLE), configuration);
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
}
