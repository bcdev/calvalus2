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

package com.bc.calvalus.processing.l3;

import com.bc.calvalus.commons.WorkflowException;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.hadoop.MultiFileSingleBlockInputFormat;
import com.bc.ceres.binding.BindingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * A workflow item creating a Hadoop job for n input products processed to a single L3 product.
 */
public class L3WorkflowItem extends HadoopWorkflowItem {

    public L3WorkflowItem(HadoopProcessingService processingService, String jobName, Configuration jobConfig) {
        super(processingService, jobName, jobConfig);
    }

    public String getMinDate() {
        return getJobConfig().get(JobConfigNames.CALVALUS_MIN_DATE);
    }

    public String getMaxDate() {
        return getJobConfig().get(JobConfigNames.CALVALUS_MAX_DATE);
    }

    @Override
    public String getOutputDir() {
        return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR);
    }

    public L3Config getL3Config() throws WorkflowException {
        try {
            String xml = getJobConfig().get(JobConfigNames.CALVALUS_L3_PARAMETERS);
            return L3Config.fromXml(xml);
        } catch (BindingException e) {
            throw new WorkflowException("Illegal L3 parameters: " + e.getMessage(), e);
        }
    }


    @Override
    protected String[][] getJobConfigDefaults() {
        return new String[][]{
                {JobConfigNames.CALVALUS_INPUT, NO_DEFAULT},
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_L2_BUNDLE, null},
                {JobConfigNames.CALVALUS_L2_OPERATOR, null},
                {JobConfigNames.CALVALUS_L2_PARAMETERS, "<parameters/>"},
                {JobConfigNames.CALVALUS_L3_PARAMETERS, NO_DEFAULT},
                {JobConfigNames.CALVALUS_REGION_GEOMETRY, NO_DEFAULT},
                {JobConfigNames.CALVALUS_MIN_DATE, NO_DEFAULT},
                {JobConfigNames.CALVALUS_MAX_DATE, NO_DEFAULT}
        };
    }

    @Override
    protected void configureJob(Job job) throws IOException {

        Configuration jobConfig = job.getConfiguration();

        jobConfig.setIfUnset("calvalus.system.beam.reader.tileHeight", "64");
        jobConfig.setIfUnset("calvalus.system.beam.reader.tileWidth", "*");
        jobConfig.setIfUnset("calvalus.system.beam.imageManager.enableSourceTileCaching", "true");

        job.setInputFormatClass(MultiFileSingleBlockInputFormat.class);
        job.setNumReduceTasks(4);
        job.setMapperClass(L3Mapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(L3SpatialBin.class);
        job.setPartitionerClass(L3Partitioner.class);
        job.setReducerClass(L3Reducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(L3TemporalBin.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        JobUtils.clearAndSetOutputDir(job, getOutputDir());
        ProcessorFactory.installProcessor(jobConfig);
    }

}
