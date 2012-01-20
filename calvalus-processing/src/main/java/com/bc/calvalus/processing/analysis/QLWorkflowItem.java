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

package com.bc.calvalus.processing.analysis;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.beam.SimpleOutputFormat;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.mosaic.DirectoryFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * A workflow item creating a Hadoop job for creating quick-look-images
 */
public class QLWorkflowItem extends HadoopWorkflowItem {

    public QLWorkflowItem(HadoopProcessingService processingService, String jobName, Configuration jobConfig) {
        super(processingService, jobName, jobConfig);
    }

    public String getInputDir() {
        return getJobConfig().get(JobConfigNames.CALVALUS_INPUT);
    }

    @Override
    public String getOutputDir() {
        return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR);
    }

    @Override
    protected String[][] getJobConfigDefaults() {
        return new String[][]{
                {JobConfigNames.CALVALUS_INPUT, NO_DEFAULT},
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_QUICKLOOK_PARAMETERS, NO_DEFAULT},
                {JobConfigNames.CALVALUS_REGION_GEOMETRY, null},
        };
    }

    protected void configureJob(Job job) throws IOException {

        job.setInputFormatClass(DirectoryFileInputFormat.class);
        FileInputFormat.setInputPaths(job, getInputDir());
        job.setMapperClass(QLMapper.class);

        job.setNumReduceTasks(0);
        job.setOutputFormatClass(SimpleOutputFormat.class);

        JobUtils.clearAndSetOutputDir(job, getOutputDir());
    }
}
