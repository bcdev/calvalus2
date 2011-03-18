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

package com.bc.calvalus.processing.beam;

import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.shellexec.ExecutablesInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.esa.beam.util.StringUtils;

import java.io.IOException;

/**
 * A workflow item creating a Hadoop job for n input products processed to n output products using a BEAM GPF operator.
 */
public class L2WorkflowItem extends HadoopWorkflowItem {

    private final String jobName;
    private final String[] inputFiles;
    private final String outputDir;
    private final String processorBundle;
    private final String processorName;
    private final String processorParameters;

    public L2WorkflowItem(HadoopProcessingService processingService,
                          String jobName,
                          String[] inputFiles,
                          String outputDir,
                          String processorBundle,
                          String processorName,
                          String processorParameters) {
        super(processingService);
        this.jobName = jobName;
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
        configuration.set(JobConfNames.CALVALUS_BUNDLE, processorBundle); // only informal
        configuration.set(JobConfNames.CALVALUS_L2_OPERATOR, this.processorName);
        configuration.set(JobConfNames.CALVALUS_L2_PARAMETER, this.processorParameters);

        setAndClearOutputDir(job, this.outputDir);

        job.setInputFormatClass(ExecutablesInputFormat.class);
        job.setMapperClass(BeamOperatorMapper.class);
        job.setNumReduceTasks(0);

        HadoopProcessingService.addBundleToClassPath(processorBundle, configuration);
        return job;
    }
}
