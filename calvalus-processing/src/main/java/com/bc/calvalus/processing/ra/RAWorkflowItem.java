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

package com.bc.calvalus.processing.ra;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.beam.SimpleOutputFormat;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * A workflow item creating a Hadoop job for region analysis
 *
 * @author MarcoZ
 */
public class RAWorkflowItem extends HadoopWorkflowItem {

    public RAWorkflowItem(HadoopProcessingService processingService, String username, String jobName, Configuration jobConfig) {
        super(processingService, username, jobName, jobConfig);
    }

    @Override
    public String getOutputDir() {
        return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR);
    }

    @Override
    protected String[][] getJobConfigDefaults() {
        return new String[][]{
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_L2_PARAMETERS, "<parameters/>"},
                {JobConfigNames.CALVALUS_RA_PARAMETERS, NO_DEFAULT},
                {JobConfigNames.CALVALUS_REGION_GEOMETRY, NO_DEFAULT},
        };
    }

    @Override
    protected void configureJob(Job job) throws IOException {

        Configuration jobConfig = job.getConfiguration();

        // Note: these are applied in GpfUtils.init().
        jobConfig.setIfUnset("calvalus.system.snap.dataio.reader.tileWidth", "*");
        jobConfig.setIfUnset("calvalus.system.snap.dataio.reader.tileHeight", "64");
        jobConfig.setIfUnset("calvalus.system.snap.pixelGeoCoding.useTiling", "true");
        jobConfig.setIfUnset("calvalus.system.snap.envisat.usePixelGeoCoding", "true");
        jobConfig.setIfUnset("calvalus.system.snap.pixelGeoCoding.fractionAccuracy", "true");


        job.setInputFormatClass(getInputFormatClass(jobConfig));

        job.setMapperClass(RAMapper.class);
        job.setMapOutputKeyClass(RAKey.class);
        job.setMapOutputValueClass(RAValue.class);

        job.setPartitionerClass(RAPartitioner.class);
        job.setGroupingComparatorClass(RAKey.GroupComparator.class);

        job.setNumReduceTasks(1); // TODO
        job.setReducerClass(RAReducer.class);
        job.setOutputFormatClass(SimpleOutputFormat.class);

        JobUtils.clearAndSetOutputDir(getOutputDir(), job, this);
    }

}
