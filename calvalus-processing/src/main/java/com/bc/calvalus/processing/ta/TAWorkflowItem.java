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

package com.bc.calvalus.processing.ta;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.WorkflowStatusEvent;
import com.bc.calvalus.commons.WorkflowStatusListener;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.l3.L3TemporalBin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * A workflow item that outputs a sequence file of {@link TAPoint}s from a L3 output sequence file.
 *
 * @author Norman
 */
public class TAWorkflowItem extends HadoopWorkflowItem {

    public TAWorkflowItem(HadoopProcessingService processingService, String jobName, Configuration jobConfig) {
        super(processingService, jobName, jobConfig);
    }

    public String getInputDir() {
        return getJobConfig().get(JobConfigNames.CALVALUS_INPUT_DIR);
    }

    @Override
    public String getOutputDir() {
        return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR);
    }

    public String getMinDate() {
        return getJobConfig().get(JobConfigNames.CALVALUS_MIN_DATE);
    }

    public String getMaxDate() {
        return getJobConfig().get(JobConfigNames.CALVALUS_MAX_DATE);
    }

    @Override
    protected String[][] getJobConfigDefaults() {
        return new String[][]{
                {JobConfigNames.CALVALUS_INPUT_DIR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_L3_PARAMETERS, NO_DEFAULT},
                {JobConfigNames.CALVALUS_L3_COMPUTE_OUTPUTS, "false"},
                {JobConfigNames.CALVALUS_TA_PARAMETERS, NO_DEFAULT},
                {JobConfigNames.CALVALUS_MIN_DATE, NO_DEFAULT},
                {JobConfigNames.CALVALUS_MAX_DATE, NO_DEFAULT}
        };
    }

    @Override
    protected void configureJob(Job job) throws IOException {

        FileInputFormat.addInputPath(job, new Path(getInputDir()));
        job.setInputFormatClass(SequenceFileInputFormat.class);

        JobUtils.clearAndSetOutputDir(getOutputDir(), job);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(TAMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(L3TemporalBin.class);
        job.setReducerClass(TAReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TAPoint.class);
        job.setNumReduceTasks(1);

        // If this item is completed, clear L3 output dir, which is not used anymore
        addWorkflowStatusListener(new InputDirCleaner(job));
    }

    private class InputDirCleaner implements WorkflowStatusListener {

        private final Job job;

        public InputDirCleaner(Job job) {
            this.job = job;
        }

        @Override
        public void handleStatusChanged(WorkflowStatusEvent event) {
            if (event.getSource() == TAWorkflowItem.this
                && event.getNewStatus().getState() == ProcessState.COMPLETED) {
                clearInputDir(job);
            }
        }

        private void clearInputDir(Job job) {
            try {
                JobUtils.clearDir(getInputDir(), job);
            } catch (IOException e) {
                // todo - nf/** 19.04.2011: log error
            }
        }
    }
}
