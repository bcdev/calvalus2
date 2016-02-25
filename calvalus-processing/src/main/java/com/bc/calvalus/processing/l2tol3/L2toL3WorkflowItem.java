/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.l2tol3;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.SimpleOutputFormat;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.hadoop.PatternBasedInputFormat;
import com.bc.calvalus.processing.hadoop.TableInputFormat;
import com.bc.calvalus.processing.l3.L3SpatialBin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * A workflow item creating a Hadoop job for n input products processed to a single L3 product.
 */
public class L2toL3WorkflowItem extends HadoopWorkflowItem {

    public L2toL3WorkflowItem(HadoopProcessingService processingService, String username, String jobName, Configuration jobConfig) {
        super(processingService, username, jobName, jobConfig);
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

    @Override
    protected String[][] getJobConfigDefaults() {
        return new String[][]{
                /* {JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, NO_DEFAULT}, */
                {JobConfigNames.CALVALUS_INPUT_REGION_NAME, null},
                {JobConfigNames.CALVALUS_INPUT_DATE_RANGES, null},
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_BUNDLES, null},
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

        jobConfig.setIfUnset("calvalus.system.snap.dataio.reader.tileHeight", "64");
        jobConfig.setIfUnset("calvalus.system.snap.dataio.reader.tileWidth", "*");
        jobConfig.setIfUnset("calvalus.system.beam.reader.tileHeight", "64");
        jobConfig.setIfUnset("calvalus.system.beam.reader.tileWidth", "*");

        if (job.getConfiguration().get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS) != null) {
            job.setInputFormatClass(PatternBasedInputFormat.class);
        } else if (job.getConfiguration().get(JobConfigNames.CALVALUS_INPUT_TABLE) != null) {
            job.setInputFormatClass(TableInputFormat.class);
        } else {
            throw new IOException("missing job parameter " + JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS +
                                          " or " + JobConfigNames.CALVALUS_INPUT_TABLE);
        }
        job.setMapperClass(L2toL3Mapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(L3SpatialBin.class);

        job.setNumReduceTasks(1);
        job.setReducerClass(L2toL3Reducer.class);
        job.setOutputFormatClass(SimpleOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(getOutputDir()));
    }

}
