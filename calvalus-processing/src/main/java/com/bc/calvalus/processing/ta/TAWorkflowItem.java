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

package com.bc.calvalus.processing.ta;

import com.bc.calvalus.binning.SpatialBin;
import com.bc.calvalus.processing.CalvalusInputFormat;
import com.bc.calvalus.processing.JobConfNames;
import com.bc.calvalus.processing.beam.BeamUtils;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.esa.beam.util.StringUtils;

import java.io.IOException;

/**
 * A workflow item creating a Hadoop job for n input products processed to a single L3 product.
 */
public class TAWorkflowItem extends HadoopWorkflowItem {

    private final String jobName;
    private final String processorBundle;
    private final String processorName;
    private final String processorParameters;
    private final String[] inputFiles;
    private final String outputDir;
    private final L3Config l3Config;
    private final Geometry roiGeometry;
    private final String startDate;
    private final String stopDate;


    public TAWorkflowItem(HadoopProcessingService processingService,
                          String jobName,
                          String processorBundle,
                          String processorName,
                          String processorParameters,
                          Geometry roiGeometry,
                          String[] inputFiles,
                          String outputDir,
                          L3Config l3Config,
                          String startDate,
                          String stopDate) {
        super(processingService);
        this.jobName = jobName;
        this.processorBundle = processorBundle;
        this.processorName = processorName;
        this.processorParameters = processorParameters;
        this.inputFiles = inputFiles;
        this.outputDir = outputDir;
        this.l3Config = l3Config;
        this.roiGeometry = roiGeometry;
        this.startDate = startDate;
        this.stopDate = stopDate;
    }

    public String getStopDate() {
        return stopDate;
    }

    public String getStartDate() {
        return startDate;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public L3Config getL3Config() {
        return l3Config;
    }

    public Geometry getRoiGeometry() {
        return roiGeometry;
    }

    protected Job createJob() throws IOException {

        Job job = getProcessingService().createJob(jobName);
        Configuration configuration = job.getConfiguration();

        configuration.set(JobConfNames.CALVALUS_INPUT, StringUtils.join(inputFiles, ","));
        configuration.set(JobConfNames.CALVALUS_OUTPUT, outputDir);
        configuration.set(JobConfNames.CALVALUS_BUNDLE, processorBundle); // only informal
        configuration.set(JobConfNames.CALVALUS_L2_OPERATOR, processorName);
        configuration.set(JobConfNames.CALVALUS_L2_PARAMETERS, processorParameters);
        configuration.set(JobConfNames.CALVALUS_REGION_GEOMETRY, roiGeometry != null ? roiGeometry.toString() : "");
        configuration.set(JobConfNames.CALVALUS_L3_PARAMETERS, BeamUtils.convertObjectToXml(l3Config));

        setAndClearOutputDir(job, outputDir);

        job.setInputFormatClass(CalvalusInputFormat.class);
        job.setNumReduceTasks(4);
        job.setMapperClass(TAMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(SpatialBin.class);
        job.setReducerClass(TAReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TAPoint.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        HadoopProcessingService.addBundleToClassPath(processorBundle, configuration);
        return job;
    }

}
