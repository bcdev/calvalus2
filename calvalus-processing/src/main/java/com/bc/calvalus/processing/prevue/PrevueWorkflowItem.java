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

package com.bc.calvalus.processing.prevue;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.hadoop.PatternBasedInputFormat;
import com.bc.calvalus.processing.hadoop.TableInputFormat;
import com.bc.calvalus.processing.snap.SimpleOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * A workflow item creating a Hadoop job for "Prevue"-FSG and RR extraction on n input products.
 *
 * @author MarcoZ
 */
public class PrevueWorkflowItem extends HadoopWorkflowItem {

    public PrevueWorkflowItem(HadoopProcessingService processingService, String username, String jobName, Configuration jobConfig) {
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
                {JobConfigNames.CALVALUS_INPUT_FORMAT, null},
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_MA_PARAMETERS, NO_DEFAULT},
        };
    }

    @Override
    protected void configureJob(Job job) throws IOException {

        Configuration jobConfig = job.getConfiguration();

        // Note: these are applied in GpfUtils.init().
        jobConfig.setIfUnset("calvalus.system.snap.dataio.reader.tileWidth", "50");
        jobConfig.setIfUnset("calvalus.system.snap.dataio.reader.tileHeight", "50");
        jobConfig.setIfUnset("calvalus.system.snap.pixelGeoCoding.useTiling", "true");
        jobConfig.setIfUnset("calvalus.system.snap.envisat.usePixelGeoCoding", "true");

        if (job.getConfiguration().get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS) != null) {
            job.setInputFormatClass(PatternBasedInputFormat.class);
        } else if (job.getConfiguration().get(JobConfigNames.CALVALUS_INPUT_TABLE) != null) {
            job.setInputFormatClass(TableInputFormat.class);
        } else {
            throw new IOException("missing job parameter " + JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS +
                                          " or " + JobConfigNames.CALVALUS_INPUT_TABLE);
        }
        job.setMapperClass(PrevueMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(SimpleOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(getOutputDir()));
    }
}
