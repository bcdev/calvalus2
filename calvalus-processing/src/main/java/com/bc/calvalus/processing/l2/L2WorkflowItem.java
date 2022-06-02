/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.l2;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.SimpleOutputFormat;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * A workflow item creating a Hadoop job for n input products processed to n output products using a SNAP GPF operator.
 */
public class L2WorkflowItem extends HadoopWorkflowItem {

    private final Class mapperClass;
    private final Class outputFormatClass;

    public L2WorkflowItem(HadoopProcessingService processingService, String username, String jobName, Configuration jobConfig) {
        this(processingService, username, jobName, jobConfig, L2Mapper.class, SimpleOutputFormat.class);
    }

    public L2WorkflowItem(HadoopProcessingService processingService, String username, String jobName, Configuration jobConfig,
                          Class mapperClass, Class outputFormatClass) {
        super(processingService, username, jobName, jobConfig);
        this.mapperClass = mapperClass;
        this.outputFormatClass = outputFormatClass;
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
                {JobConfigNames.CALVALUS_INPUT_MIN_WIDTH, "0"},
                {JobConfigNames.CALVALUS_INPUT_MIN_HEIGHT, "0"},
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_BUNDLES, null},
                {JobConfigNames.CALVALUS_L2_OPERATOR, null},
                {JobConfigNames.CALVALUS_L2_PARAMETERS, "<parameters/>"},
                {JobConfigNames.CALVALUS_REGION_GEOMETRY, null},
                {JobConfigNames.CALVALUS_PROCESS_ALL, "false"}
        };
    }

    protected void configureJob(Job job) throws IOException {
        Configuration jobConfig = job.getConfiguration();

        jobConfig.setIfUnset("calvalus.system.snap.dataio.reader.tileHeight", "64");
        jobConfig.setIfUnset("calvalus.system.snap.dataio.reader.tileWidth", "*");
        jobConfig.setIfUnset("calvalus.system.beam.reader.tileHeight", "64");
        jobConfig.setIfUnset("calvalus.system.beam.reader.tileWidth", "*");

        job.setInputFormatClass(getInputFormatClass(jobConfig));
        job.setMapperClass(mapperClass);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(outputFormatClass);

        FileOutputFormat.setOutputPath(job, new Path(getOutputDir()));
    }
}
