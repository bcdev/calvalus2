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

package com.bc.calvalus.processing.ma;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.hadoop.PatternBasedInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * A workflow item creating a Hadoop job for match-up extraction on n input products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class MAWorkflowItem extends HadoopWorkflowItem {

    public MAWorkflowItem(HadoopProcessingService processingService, String username, String jobName, Configuration jobConfig) {
        super(processingService, username, jobName, jobConfig);
    }

    @Override
    public String getOutputDir() {
        return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR);
    }

    @Override
    protected String[][] getJobConfigDefaults() {
        return new String[][]{
                {JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, NO_DEFAULT},
                {JobConfigNames.CALVALUS_INPUT_REGION_NAME, null},
                {JobConfigNames.CALVALUS_INPUT_DATE_RANGES, null},
                {JobConfigNames.CALVALUS_INPUT_FORMAT, null},
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_L2_BUNDLE, null},
                {JobConfigNames.CALVALUS_L2_BUNDLE_LOCATION, null},
                {JobConfigNames.CALVALUS_L2_OPERATOR, null},
                {JobConfigNames.CALVALUS_L2_PARAMETERS, "<parameters/>"},
                {JobConfigNames.CALVALUS_MA_PARAMETERS, NO_DEFAULT},
                {JobConfigNames.CALVALUS_REGION_GEOMETRY, null},
        };
    }

    @Override
    protected void configureJob(Job job) throws IOException {

        Configuration jobConfig = job.getConfiguration();

        // Note: these are applied in GpfUtils.init().
        jobConfig.setIfUnset("calvalus.system.beam.reader.tileWidth", "32");
        jobConfig.setIfUnset("calvalus.system.beam.reader.tileHeight", "32");
        jobConfig.setIfUnset("calvalus.system.beam.pixelGeoCoding.useTiling", "true");
        jobConfig.setIfUnset("calvalus.system.beam.envisat.usePixelGeoCoding", "true");
        jobConfig.setIfUnset("calvalus.system.beam.pixelGeoCoding.fractionAccuracy", "true");


        // Enable JVM reuse, to speedup processing.
        // Maybe apply to other workflow-items in the future.
        // jobConfig.setInt("mapred.job.reuse.jvm.num.tasks", 10);
        // disabled
        jobConfig.setInt("mapred.job.reuse.jvm.num.tasks", 1);

        job.setInputFormatClass(PatternBasedInputFormat.class);
        job.setMapperClass(MAMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RecordWritable.class);
        job.setReducerClass(MAReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(RecordWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        JobUtils.clearAndSetOutputDir(getOutputDir(), job, this);
        ProcessorFactory.installProcessorBundle(jobConfig);
    }

}
