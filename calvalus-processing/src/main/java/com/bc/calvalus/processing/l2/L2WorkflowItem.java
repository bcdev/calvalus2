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

import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.hadoop.MultiFileSingleBlockInputFormat;
import com.bc.calvalus.processing.JobConfNames;
import com.bc.calvalus.processing.beam.BeamOperatorMapper;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.esa.beam.util.StringUtils;

import java.io.IOException;
import java.util.Properties;

/**
 * A workflow item creating a Hadoop job for n input products processed to n output products using a BEAM GPF operator.
 */
public class L2WorkflowItem extends HadoopWorkflowItem {

    private final String jobName;
    private final Geometry regionGeometry;
    private final String[] inputFiles;
    private final String outputDir;
    private final String processorBundle;
    private final String processorName;
    private final String processorParameters;

    public L2WorkflowItem(HadoopProcessingService processingService,
                          String jobName,
                          String processorBundle,
                          String processorName,
                          String processorParameters,
                          Geometry regionGeometry,
                          String[] inputFiles,
                          String outputDir) {
        super(processingService);
        this.jobName = jobName;
        this.regionGeometry = regionGeometry;
        this.inputFiles = inputFiles;
        this.outputDir = outputDir;
        this.processorBundle = processorBundle;
        this.processorName = processorName;
        this.processorParameters = processorParameters;
    }

    public String getOutputDir() {
        return outputDir;
    }

    protected Job createJob() throws IOException {
        Job job = getProcessingService().createJob(jobName);
        Configuration configuration = job.getConfiguration();

        configuration.set(JobConfNames.CALVALUS_INPUT, StringUtils.join(this.inputFiles, ","));
        configuration.set(JobConfNames.CALVALUS_OUTPUT, this.outputDir);
        configuration.set(JobConfNames.CALVALUS_L2_BUNDLE, processorBundle);
        configuration.set(JobConfNames.CALVALUS_L2_OPERATOR, this.processorName);
        configuration.set(JobConfNames.CALVALUS_L2_PARAMETERS, this.processorParameters);
        configuration.set(JobConfNames.CALVALUS_REGION_GEOMETRY, regionGeometry != null ? regionGeometry.toString() : "");

        Properties properties = new Properties();
        properties.setProperty("beam.envisat.tileHeight", "64");
        properties.setProperty("beam.envisat.tileWidth", "*");
        properties.setProperty("beam.reader.tileHeight", "64");
        properties.setProperty("beam.reader.tileWidth", "*");
        String propertiesString = JobUtils.convertProperties(properties);
        configuration.set(JobConfNames.CALVALUS_SYSTEM_PROPERTIES, propertiesString);

        JobUtils.clearAndSetOutputDir(job, this.outputDir);

        job.setInputFormatClass(MultiFileSingleBlockInputFormat.class);
        job.setMapperClass(BeamOperatorMapper.class);
        job.setNumReduceTasks(0);

        HadoopProcessingService.addBundleToClassPath(processorBundle, configuration);
        return job;
    }
}
