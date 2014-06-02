/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.boostrapping;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * A workflow item creating a Hadoop job for bootstrapping analysis.
 *
 * @author MarcoZ
 * @author MarcoP
 */
public class BootstrappingWorkflowItem extends HadoopWorkflowItem {

    public static final String NUM_ITERATIONS_PROPERTY = "calvalus.bootstrap.numberOfIterations";
    public static final String ITERATION_PER_NODE_PROPERTY = "calvalus.bootstrap.iterationsPerNode";
    public static final String INPUT_FILE_PROPRTY = "calvalus.bootstrap.inputFile";

    public static final int NUM_ITERATIONS_DEFAULT = 10000;
    public static final int ITERATION_PER_NODE_DEFAULT = 100;

    public BootstrappingWorkflowItem(HadoopProcessingService processingService, String username, String jobName, Configuration jobConfig) {
        super(processingService, username, jobName, jobConfig);
    }

    @Override
    public String getOutputDir() {
        return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR);
    }

    @Override
    protected String[][] getJobConfigDefaults() {
        return new String[][]{
                {JobConfigNames.CALVALUS_L2_BUNDLE, null},
                {JobConfigNames.CALVALUS_L2_BUNDLE_LOCATION, null},
                {JobConfigNames.CALVALUS_L2_OPERATOR, null},
                {INPUT_FILE_PROPRTY, NO_DEFAULT},
                {NUM_ITERATIONS_PROPERTY, Integer.toString(NUM_ITERATIONS_DEFAULT)},
                {ITERATION_PER_NODE_PROPERTY, Integer.toString(ITERATION_PER_NODE_DEFAULT)},
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
        };
    }

    @Override
    protected void configureJob(Job job) throws IOException {

        Configuration jobConfig = job.getConfiguration();

        job.setInputFormatClass(NtimesInputFormat.class);
        job.setMapperClass(BootstrappingMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(BoostrappingReducer.class);
        job.setNumReduceTasks(1);

        JobUtils.clearAndSetOutputDir(getOutputDir(), job);
        ProcessorFactory.installProcessorBundle(jobConfig);
    }

}
