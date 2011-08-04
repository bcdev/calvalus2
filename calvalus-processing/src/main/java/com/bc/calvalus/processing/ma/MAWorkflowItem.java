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
import com.bc.calvalus.processing.beam.BeamUtils;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.hadoop.MultiFileSingleBlockInputFormat;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
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

    private final String jobName;
//    private final String processorBundle;
//    private final String processorName;
//    private final String processorParameters;
    private final String[] inputFiles;
    private final String outputDir;
    private final MAConfig maConfig;
//    private final Geometry roiGeometry;
//    private final String minDate;
//    private final String maxDate;

    public MAWorkflowItem(HadoopProcessingService processingService,
                          String jobName,
//                          String processorBundle,
//                          String processorName,
//                          String processorParameters,
//                          Geometry roiGeometry,
                          String[] inputFiles,
                          String outputDir,
                          MAConfig maConfig
//                          String minDate,
//                          String maxDate
    ) {
        super(processingService);
        this.jobName = jobName;
//        this.processorBundle = processorBundle;
//        this.processorName = processorName;
//        this.processorParameters = processorParameters;
        this.inputFiles = inputFiles;
        this.outputDir = outputDir;
        this.maConfig = maConfig;
//        this.roiGeometry = roiGeometry;
//        this.minDate = minDate;
//        this.maxDate = maxDate;
    }

    public String getOutputDir() {
        return outputDir;
    }

    protected Job createJob() throws IOException {

        Job job = getProcessingService().createJob(jobName);
        Configuration configuration = job.getConfiguration();

        configuration.set(JobConfNames.CALVALUS_INPUT, StringUtils.join(inputFiles, ","));
        configuration.set(JobConfNames.CALVALUS_OUTPUT, outputDir);

//        configuration.set(JobConfNames.CALVALUS_L2_BUNDLE, processorBundle);
//        configuration.set(JobConfNames.CALVALUS_L2_OPERATOR, processorName);
//        configuration.set(JobConfNames.CALVALUS_L2_PARAMETERS, processorParameters);

        configuration.set(JobConfNames.CALVALUS_MA_PARAMETERS, BeamUtils.convertObjectToXml(maConfig));

//        configuration.set(JobConfNames.CALVALUS_REGION_GEOMETRY, roiGeometry != null ? roiGeometry.toString() : "");
//        configuration.set(JobConfNames.CALVALUS_MIN_DATE, minDate);
//        configuration.set(JobConfNames.CALVALUS_MAX_DATE, maxDate);

        Properties properties = new Properties();
        properties.setProperty("beam.reader.tileHeight", "64");
        properties.setProperty("beam.reader.tileWidth", "*");
        String propertiesString = JobUtils.convertProperties(properties);
        configuration.set(JobConfNames.CALVALUS_SYSTEM_PROPERTIES, propertiesString);

        JobUtils.clearAndSetOutputDir(job, outputDir);

        job.setInputFormatClass(MultiFileSingleBlockInputFormat.class);

        job.setMapperClass(MAMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RecordWritable.class);

//        job.setPartitionerClass(L3Partitioner.class);
//        job.setNumReduceTasks(4);
//        job.setReducerClass(L3Reducer.class);
//        job.setOutputKeyClass(LongWritable.class);
//        job.setOutputValueClass(TemporalBin.class);

        job.setOutputFormatClass(TextOutputFormat.class);

//        HadoopProcessingService.addBundleToClassPath(processorBundle, configuration);
        return job;
    }

}
