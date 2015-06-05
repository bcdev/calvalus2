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

package com.bc.calvalus.processing.vc;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.hadoop.PatternBasedInputFormat;
import com.bc.calvalus.processing.ma.MAReducer;
import com.bc.calvalus.processing.ma.RecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;


/**
 * A workflow item creating a Hadoop job for computation of vicarious calibration coefficients on n input products.
 *
 * @author MarcoZ
 */
public class VCWorkflowItem extends HadoopWorkflowItem {

    public static final String DIFFERENTIATION_SUFFIX = ".differentiation";

    public VCWorkflowItem(HadoopProcessingService processingService, String userName, String jobName, Configuration jobConfig) {
        super(processingService, userName, jobName, jobConfig);
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
                {JobConfigNames.CALVALUS_INPUT_FORMAT, null},
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},

                {JobConfigNames.CALVALUS_BUNDLES, null},
                {JobConfigNames.CALVALUS_L2_OPERATOR, null},
                {JobConfigNames.CALVALUS_L2_PARAMETERS, "<parameters/>"},

                {JobConfigNames.CALVALUS_BUNDLES + DIFFERENTIATION_SUFFIX, null},
                {JobConfigNames.CALVALUS_L2_OPERATOR + DIFFERENTIATION_SUFFIX, null},
                {JobConfigNames.CALVALUS_L2_PARAMETERS + DIFFERENTIATION_SUFFIX, "<parameters/>"},

                {JobConfigNames.CALVALUS_MA_PARAMETERS, NO_DEFAULT},
                {JobConfigNames.CALVALUS_REGION_GEOMETRY, null},
        };
    }

    @Override
    protected void configureJob(Job job) throws IOException {

        Configuration jobConfig = job.getConfiguration();

        // Note: these are applied in GpfUtils.init().
        jobConfig.setIfUnset("calvalus.system.beam.reader.tileWidth", "32");
        jobConfig.setIfUnset("calvalus.system.beam.reader.tileHeight", "32");
        jobConfig.setIfUnset("calvalus.system.beam.pixelGeoCoding.useTiling", "true");
        jobConfig.setIfUnset("calvalus.system.beam.envisat.usePixelGeoCoding", "true");
        jobConfig.setIfUnset("calvalus.system.beam.pixelGeoCoding.fractionAccuracy", "true");

        job.setInputFormatClass(PatternBasedInputFormat.class);
        job.setMapperClass(VCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RecordWritable.class);
        job.setReducerClass(MAReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(RecordWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        JobUtils.clearAndSetOutputDir(getOutputDir(), job, this);
        ProcessorFactory.installProcessorBundles(jobConfig);
    }

}

