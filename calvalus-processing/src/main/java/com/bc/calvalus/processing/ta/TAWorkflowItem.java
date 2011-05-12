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

import com.bc.calvalus.binning.SpatialBin;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.WorkflowStatusEvent;
import com.bc.calvalus.commons.WorkflowStatusListener;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.JobConfNames;
import com.bc.calvalus.processing.beam.BeamUtils;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * A workflow item that outputs a sequence file of {@link TAPoint}s from a L3 output sequence file.
 *
 * @author Norman
 */
public class TAWorkflowItem extends HadoopWorkflowItem {

    private final String jobName;
    private final String inputDir;
    private final String outputDir;
    private final L3Config l3Config;
    private final TAConfig taConfig;
    private final String minDate;
    private final String maxDate;

    public TAWorkflowItem(HadoopProcessingService processingService,
                          String jobName,
                          String inputDir,
                          String outputDir,
                          L3Config l3Config,
                          TAConfig taConfig,
                          String minDate,
                          String maxDate) {
        super(processingService);
        this.jobName = jobName;
        this.inputDir = inputDir;
        this.outputDir = outputDir;
        this.l3Config = l3Config;
        this.taConfig = taConfig;
        this.minDate = minDate;
        this.maxDate = maxDate;
    }

    public String getMaxDate() {
        return maxDate;
    }

    public String getMinDate() {
        return minDate;
    }

    public String getInputDir() {
        return inputDir;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public L3Config getL3Config() {
        return l3Config;
    }

    protected Job createJob() throws IOException {

        final Job job = getProcessingService().createJob(jobName);
        Configuration configuration = job.getConfiguration();

        configuration.set(JobConfNames.CALVALUS_OUTPUT, outputDir);
        configuration.set(JobConfNames.CALVALUS_L3_PARAMETERS, BeamUtils.convertObjectToXml(l3Config));
        configuration.set(JobConfNames.CALVALUS_TA_PARAMETERS, BeamUtils.convertObjectToXml(taConfig));
        configuration.set(JobConfNames.CALVALUS_MIN_DATE, minDate);
        configuration.set(JobConfNames.CALVALUS_MAX_DATE, maxDate);

        SequenceFileInputFormat.addInputPath(job, new Path(inputDir));
        job.setInputFormatClass(SequenceFileInputFormat.class);

        JobUtils.clearAndSetOutputDir(job, outputDir);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setNumReduceTasks(1);
        job.setMapperClass(TAMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(SpatialBin.class);
        job.setReducerClass(TAReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TAPoint.class);

        // If this item is completed, clear L3 output dir, which is not used anymore
        addWorkflowStatusListener(new InputDirCleaner(job));

        return job;
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
                JobUtils.clearDir(job, inputDir);
            } catch (IOException e) {
                // todo - nf/** 19.04.2011: log error
            }
        }
    }
}
