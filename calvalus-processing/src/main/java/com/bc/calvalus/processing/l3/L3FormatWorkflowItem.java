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

package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.beam.SimpleOutputFormat;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * A workflow item creating a Hadoop job for formatting the result of an L3 job into a single L3 product.
 */
public class L3FormatWorkflowItem extends HadoopWorkflowItem {

    public L3FormatWorkflowItem(HadoopProcessingService processingService, String jobName, Configuration jobConfig) {
        super(processingService, jobName, jobConfig);
    }

    public String getInputDir() {
        return getJobConfig().get(JobConfigNames.CALVALUS_INPUT_DIR);
    }

    @Override
    public String getOutputDir() {
        return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR);
    }

    @Override
    protected String[][] getJobConfigDefaults() {
        return new String[][]{
                {JobConfigNames.CALVALUS_INPUT_DIR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_OUTPUT_PREFIX, "L3"},
                {JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF4"},
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_L3_PARAMETERS, NO_DEFAULT},
                {JobConfigNames.CALVALUS_REGION_GEOMETRY, NO_DEFAULT},
                {JobConfigNames.CALVALUS_MIN_DATE, NO_DEFAULT},
                {JobConfigNames.CALVALUS_MAX_DATE, NO_DEFAULT}
        };
    }

    @Override
    protected void configureJob(Job job) throws IOException {

        job.setInputFormatClass(L3FormatterInputFormat.class);
        FileInputFormat.setInputPaths(job, getInputDir());

        job.setMapperClass(L3FormatterMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(SimpleOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(getOutputDir()));
        ProcessorFactory.installProcessorBundle(job.getConfiguration());
    }

}
