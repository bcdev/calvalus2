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

package com.bc.calvalus.processing.l2;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.beam.BeamOperatorMapper;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.hadoop.MultiFileSingleBlockInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * A workflow item creating a Hadoop job for n input products processed to n output products using a BEAM GPF operator.
 */
public class L2WorkflowItem extends HadoopWorkflowItem {

    public L2WorkflowItem(HadoopProcessingService processingService, String jobName, Configuration jobConfig) {
        super(processingService, jobName, jobConfig);
    }

    public String getInputFiles() {
        return getJobConfig().get(JobConfigNames.CALVALUS_INPUT);
    }

    public String getOutputDir() {
        return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR);
    }

    public String getProcessorBundle() {
        return getJobConfig().get(JobConfigNames.CALVALUS_L2_BUNDLE);
    }


    @Override
    protected String[][] getJobConfigDefaults() {
        return new String[][]{
                {JobConfigNames.CALVALUS_INPUT, NO_DEFAULT},
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_L2_BUNDLE, NO_DEFAULT},
                {JobConfigNames.CALVALUS_L2_OPERATOR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_L2_PARAMETERS, "<parameters/>"},
                {JobConfigNames.CALVALUS_REGION_GEOMETRY, null}
        };
    }

    protected void configureJob(Job job) throws IOException {
        Configuration jobConfig = job.getConfiguration();

        jobConfig.set("calvalus.system.beam.reader.tileHeight", "64");
        jobConfig.set("calvalus.system.beam.reader.tileWidth", "*");
        jobConfig.set("calvalus.system.beam.imageManager.enableSourceTileCaching", "true");

        job.setInputFormatClass(MultiFileSingleBlockInputFormat.class);
        job.setMapperClass(BeamOperatorMapper.class);
        job.setNumReduceTasks(0);

        JobUtils.clearAndSetOutputDir(job, getOutputDir());
        if (getProcessorBundle() != null) {
            HadoopProcessingService.addBundleToClassPath(getProcessorBundle(), jobConfig);
        }
    }
}
