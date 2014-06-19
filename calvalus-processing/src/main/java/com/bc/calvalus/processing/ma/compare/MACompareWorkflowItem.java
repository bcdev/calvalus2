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

package com.bc.calvalus.processing.ma.compare;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * A workflow item for merging match-ups from multiple processors
 */
public class MACompareWorkflowItem extends HadoopWorkflowItem {

    public MACompareWorkflowItem(HadoopProcessingService processingService, String username, String jobName, Configuration jobConfig) {
        super(processingService, username, jobName, jobConfig);
    }

    public String getInputFiles() {
        return getJobConfig().get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS);
    }

    @Override
    public String getOutputDir() {
        return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR);
    }

    @Override
    protected String[][] getJobConfigDefaults() {
        return new String[][]{
                {JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, NO_DEFAULT},
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_MA_PARAMETERS, NO_DEFAULT},
                {"calvalus.ma.identifiers", NO_DEFAULT},
        };
    }

    @Override
    protected void configureJob(Job job) throws IOException {
        FileInputFormat.addInputPaths(job, getInputFiles());
        job.setInputFormatClass(MACompareInputFormat.class);

        job.setMapperClass(MACompareMapper.class);
        job.setMapOutputKeyClass(MAKey.class);
        job.setMapOutputValueClass(IndexedRecordWritable.class);

        job.setGroupingComparatorClass(MAKey.GroupingComparator.class);

        job.setReducerClass(MACompareReducer.class);

        job.setNumReduceTasks(1);

        JobUtils.clearAndSetOutputDir(getOutputDir(), job, this);
    }
}
