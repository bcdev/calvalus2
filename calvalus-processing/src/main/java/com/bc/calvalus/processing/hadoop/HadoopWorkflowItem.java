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
import com.bc.calvalus.processing.beam.CalvalusClasspath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.esa.beam.framework.datamodel.ProductData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * A workflow item that corresponds to a single Hadoop job.
 */
public abstract class HadoopWorkflowItem extends AbstractWorkflowItem {
    private static final String CALVALUS_HDFS_INSTALL_DIR = "calvalus-1.0-SNAPSHOT";
    private final HadoopProcessingService processingService;
    private JobID jobId;

    public HadoopWorkflowItem(HadoopProcessingService processingService) {
        this.processingService = processingService;
    }

    public HadoopProcessingService getProcessingService() {
        return processingService;
    }

    public JobID getJobId() {
        return jobId;
    }

    public void setJobId(JobID jobId) {
        this.jobId = jobId;
    }

    @Override
     public void kill() throws WorkflowException {
         try {
             processingService.killJob(jobId);
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
         return jobId != null ? new Object[]{jobId} : new Object[0];
     }


    protected JobID submitJob(Job job) throws IOException {
        Configuration configuration = job.getConfiguration();
        //add calvalus itself to classpath of hadoop jobs
        CalvalusClasspath.addPackageToClassPath(CALVALUS_HDFS_INSTALL_DIR, configuration);
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
