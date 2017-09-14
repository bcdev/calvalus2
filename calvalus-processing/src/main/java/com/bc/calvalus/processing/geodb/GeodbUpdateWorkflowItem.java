/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.geodb;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.boostrapping.BootstrappingWorkflowItem;
import com.bc.calvalus.processing.boostrapping.NtimesInputFormat;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

public class GeodbUpdateWorkflowItem extends HadoopWorkflowItem {

    public GeodbUpdateWorkflowItem(HadoopProcessingService processingService, String username, String jobName, Configuration jobConfig) {
        super(processingService, username, jobName, jobConfig);
    }

    @Override
    public String getOutputDir() {
        return getJobConfig().get(JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY);
    }

    @Override
    protected String[][] getJobConfigDefaults() {
        return new String[][]{
                {BootstrappingWorkflowItem.NUM_ITERATIONS_PROPERTY, "1"},
                {BootstrappingWorkflowItem.ITERATION_PER_NODE_PROPERTY, "1"},
                {JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY, NO_DEFAULT},
        };
    }

    protected void configureJob(Job job) throws IOException {
        // launch just a single mapper that does the update
        job.setInputFormatClass(NtimesInputFormat.class);
        job.setMapperClass(GeodbUpdateMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);
    }
}
