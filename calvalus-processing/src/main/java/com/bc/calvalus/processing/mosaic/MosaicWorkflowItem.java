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

package com.bc.calvalus.processing.mosaic;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.hadoop.MultiFileSingleBlockInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * A workflow item creating a Hadoop job for n input products processed to a single L3-Mosaic product.
 */
public class MosaicWorkflowItem extends HadoopWorkflowItem {

    public MosaicWorkflowItem(HadoopProcessingService processingService, String jobName, Configuration jobConfig) {
        super(processingService, jobName, jobConfig);
    }

    @Override
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
                {JobConfigNames.CALVALUS_L2_BUNDLE, null},
                {JobConfigNames.CALVALUS_L2_OPERATOR, null},
                {JobConfigNames.CALVALUS_L2_PARAMETERS, "<parameters/>"},
                {JobConfigNames.CALVALUS_L3_PARAMETERS, NO_DEFAULT},
                {JobConfigNames.CALVALUS_REGION_GEOMETRY, NO_DEFAULT}
        };
    }

    @Override
    protected void configureJob(Job job) throws IOException {
        Configuration jobConfig = job.getConfiguration();

        jobConfig.setIfUnset("calvalus.system.beam.reader.tileHeight", "64");
        jobConfig.setIfUnset("calvalus.system.beam.reader.tileWidth", "*");
        jobConfig.setIfUnset("calvalus.system.beam.imageManager.enableSourceTileCaching", "true");

        // because the size of the value objects can get very big
        // it is better to report progress more often
        // to prevent timeouts (Hadoop default is 10000)
        jobConfig.set("mapred.merge.recordsBeforeProgress", "10");

        job.setInputFormatClass(MultiFileSingleBlockInputFormat.class);


        job.setMapperClass(MosaicMapper.class);
        job.setMapOutputKeyClass(TileIndexWritable.class);
        job.setMapOutputValueClass(TileDataWritable.class);

        job.setPartitionerClass(MosaicPartitioner.class);

        job.setReducerClass(MosaicReducer.class);
        job.setOutputKeyClass(TileIndexWritable.class);
        job.setOutputValueClass(TileDataWritable.class);
        job.setNumReduceTasks(computeNumReducers(jobConfig));

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        JobUtils.clearAndSetOutputDir(job, getOutputDir());
        if (getProcessorBundle() != null) {
            HadoopProcessingService.addBundleToClassPath(getProcessorBundle(), jobConfig);
        }
    }

    static int computeNumReducers(Configuration jobConfig) {
        int numXPartitions = jobConfig.getInt("calvalus.mosaic.numXPartitions", 1);
        MosaicGrid mosaicGrid = MosaicGrid.create(jobConfig);
        return mosaicGrid.getNumMacroTileY() * numXPartitions;
    }

}
