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

package com.bc.calvalus.processing.l2;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.beam.SimpleOutputFormat;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.hadoop.PatternBasedInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * A workflow item creating a Hadoop job for n input products formatted to n output products.
 */
public class L2FormattingWorkflowItem extends HadoopWorkflowItem {

    public L2FormattingWorkflowItem(HadoopProcessingService processingService, String jobName,
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
                {JobConfigNames.CALVALUS_L2_BUNDLE, null},
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF"},
                {JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "gz"},
                {JobConfigNames.CALVALUS_OUTPUT_CRS, null},
                {JobConfigNames.CALVALUS_OUTPUT_BANDLIST, null},
                {JobConfigNames.CALVALUS_OUTPUT_REGEX, null},
                {JobConfigNames.CALVALUS_OUTPUT_REPLACEMENT, null},
                {JobConfigNames.CALVALUS_RESUME_PROCESSING, "false"}
        };
    }

    protected void configureJob(Job job) throws IOException {
        Configuration jobConfig = job.getConfiguration();

        jobConfig.setIfUnset("calvalus.system.beam.imageManager.enableSourceTileCaching", "true");

        job.setInputFormatClass(PatternBasedInputFormat.class);
        job.setMapperClass(L2FormatingMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(SimpleOutputFormat.class);

        // TODO add resume processing
        boolean resumeProcessing = jobConfig.getBoolean(JobConfigNames.CALVALUS_RESUME_PROCESSING, false);
        if (resumeProcessing) {
            FileOutputFormat.setOutputPath(job, new Path(getOutputDir()));
        } else {
            JobUtils.clearAndSetOutputDir(job, getOutputDir());
        }

        // for bundle only
        ProcessorFactory.installProcessor(jobConfig);
    }
}
