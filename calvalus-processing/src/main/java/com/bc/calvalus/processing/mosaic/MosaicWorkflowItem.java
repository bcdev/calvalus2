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
import com.bc.calvalus.processing.utils.GeometryUtils;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.awt.Rectangle;
import java.io.IOException;

/**
 * A workflow item creating a Hadoop job for n input products processed to a single L3-Mosaic product.
 */
public class MosaicWorkflowItem extends HadoopWorkflowItem {

    public MosaicWorkflowItem(HadoopProcessingService processingService, String username, String jobName, Configuration jobConfig) {
        super(processingService, username, jobName, jobConfig);
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
                {JobConfigNames.CALVALUS_MOSAIC_PARAMETERS, NO_DEFAULT},
                {JobConfigNames.CALVALUS_REGION_GEOMETRY, NO_DEFAULT}
        };
    }

    @Override
    protected void configureJob(Job job) throws IOException {
        Configuration jobConfig = job.getConfiguration();

        jobConfig.setIfUnset("calvalus.system.snap.dataio.reader.tileHeight", "64");
        jobConfig.setIfUnset("calvalus.system.snap.dataio.reader.tileWidth", "*");
        jobConfig.setIfUnset("calvalus.system.beam.reader.tileHeight", "64");
        jobConfig.setIfUnset("calvalus.system.beam.reader.tileWidth", "*");

        // because the size of the value objects can get very big
        // it is better to report progress more often
        // to prevent timeouts (Hadoop default is 10000)
        jobConfig.set("mapred.merge.recordsBeforeProgress", "10");

        job.setInputFormatClass(getInputFormatClass(jobConfig));

        job.setMapperClass(MosaicMapper.class);
        job.setMapOutputKeyClass(TileIndexWritable.class);
        job.setMapOutputValueClass(TileDataWritable.class);

        job.setPartitionerClass(MosaicPartitioner.class);

        job.setReducerClass(MosaicReducer.class);
        job.setOutputKeyClass(TileIndexWritable.class);
        job.setOutputValueClass(TileDataWritable.class);
        job.setNumReduceTasks(computeNumReducers(jobConfig));

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        JobUtils.clearAndSetOutputDir(getOutputDir(), job, this);
    }

    static int computeNumReducers(Configuration jobConfig) {
        Geometry regionGeometry = GeometryUtils.createGeometry(jobConfig.get("calvalus.regionGeometry"));
        if (regionGeometry != null) {
            jobConfig.set("calvalus.mosaic.regionGeometry", jobConfig.get("calvalus.regionGeometry"));
            MosaicGrid mosaicGrid = MosaicGrid.create(jobConfig);
            final int numPartitions = mosaicGrid.getNumReducers();
            System.out.println("numPartitions=" + numPartitions);
            return numPartitions;
        } else {
            MosaicGrid mosaicGrid = MosaicGrid.create(jobConfig);
            int numXPartitions = jobConfig.getInt("calvalus.mosaic.numXPartitions", 1);
            return mosaicGrid.getNumMacroTileY() * numXPartitions;
        }
    }

}
