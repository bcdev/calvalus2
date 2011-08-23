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

package com.bc.calvalus.processing.ma;

import com.bc.calvalus.processing.JobConfNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.hadoop.MultiFileSingleBlockInputFormat;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.esa.beam.util.StringUtils;

import java.io.IOException;
import java.util.Properties;

/**
 * A workflow item creating a Hadoop job for match-up extraction on n input products.
 *
 * @author MarcoZ
 *
 */
public class MAWorkflowItem extends HadoopWorkflowItem {

    private final String processorBundle;
    private final String processorName;
    private final String processorParameters;
    private final String[] inputFiles;
    private final String inputFormat;
    private final String outputDir;
    private final MAConfig maConfig;
    private final Geometry roiGeometry;
    private final String minDate;
    private final String maxDate;

    public MAWorkflowItem(HadoopProcessingService processingService,
                          String jobName,
                          String processorBundle,
                          String processorName,
                          String processorParameters,
                          Geometry roiGeometry,
                          String[] inputFiles,
                          String inputFormat,
                          String outputDir,
                          MAConfig maConfig,
                          String minDate,
                          String maxDate) {
        super(processingService, jobName);
        this.processorBundle = processorBundle;
        this.processorName = processorName;
        this.processorParameters = processorParameters;
        this.inputFiles = inputFiles;
        this.inputFormat = inputFormat;
        this.outputDir = outputDir;
        this.maConfig = maConfig;
        this.roiGeometry = roiGeometry;
        this.minDate = minDate;
        this.maxDate = maxDate;
    }

    public String getOutputDir() {
        return outputDir;
    }

    protected void configureJob(Job job) throws IOException {

        // todo - use minDate/maxDate and roiGeometry to filter input products
        // todo - use processorBundle/processorName/processorParameters to generate actual product for MA

        Configuration configuration = job.getConfiguration();

        configuration.set(JobConfNames.CALVALUS_INPUT, StringUtils.join(inputFiles, ","));
        configuration.set(JobConfNames.CALVALUS_INPUT_FORMAT, inputFormat);
        configuration.set(JobConfNames.CALVALUS_OUTPUT, outputDir);
        configuration.set(JobConfNames.CALVALUS_MA_PARAMETERS, maConfig.toXml());

//        configuration.set("calvalus.system.beam.reader.tileHeight", "64");
//        configuration.set("calvalus.system.beam.reader.tileWidth", "*");
        configuration.setBoolean("calvalus.system.beam.pixelGeoCoding.useTiling", true);
        // Enable JVM reuse, to speedup processing.
        // Maybe apply to other workflow-items in the future.
        configuration.setInt("mapred.job.reuse.jvm.num.tasks", 10);

        JobUtils.clearAndSetOutputDir(job, outputDir);

        job.setInputFormatClass(MultiFileSingleBlockInputFormat.class);

        job.setMapperClass(MAMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RecordWritable.class);

        job.setReducerClass(MAReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(RecordWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
    }

}
