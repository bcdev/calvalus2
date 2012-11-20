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

package com.bc.calvalus.processing.productinventory;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.hadoop.PatternBasedInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * A workflow item creating a Hadoop job validating n input products.
 */
public class ProductInventoryWorkflowItem extends HadoopWorkflowItem {

    public ProductInventoryWorkflowItem(HadoopProcessingService processingService, String jobName,
                                        Configuration jobConfig) {
        super(processingService, jobName, jobConfig);
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
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
        };
    }

    protected void configureJob(Job job) throws IOException {

        Configuration jobConfig = job.getConfiguration();
        jobConfig.setInt("mapred.job.reuse.jvm.num.tasks", 10);

        jobConfig.setIfUnset("calvalus.system.beam.reader.tileHeight", "64");
        jobConfig.setIfUnset("calvalus.system.beam.reader.tileWidth", "*");
        jobConfig.setIfUnset("calvalus.system.beam.imageManager.enableSourceTileCaching", "true");

        job.setInputFormatClass(PatternBasedInputFormat.class);

        job.setMapperClass(ProductInventoryMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        job.setReducerClass(ProductInventoryReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        JobUtils.clearAndSetOutputDir(job, getOutputDir());
    }
}
