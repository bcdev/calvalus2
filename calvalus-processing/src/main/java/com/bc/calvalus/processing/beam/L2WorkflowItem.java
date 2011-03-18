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

import com.bc.calvalus.commons.WorkflowException;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.shellexec.ExecutablesInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.esa.beam.util.StringUtils;

import java.io.IOException;

/**
 * A workflow item creating a Hadoop job for n input products processed to n output products using a BEAM GPF operator.
 */
public class L2WorkflowItem extends HadoopWorkflowItem {

    private final String productionId;
    private final String[] inputFiles;
    private final String outputDir;
    private final String processorBundle;
    private final String processorName;
    private final String processorParameters;

    public L2WorkflowItem(HadoopProcessingService processingService,
                          String productionId,
                          String[] inputFiles,
                          String outputDir,
                          String processorBundle,
                          String processorName,
                          String processorParameters) {
        super(processingService);
        this.productionId = productionId;
        this.inputFiles = inputFiles;
        this.outputDir = outputDir;
        this.processorBundle = processorBundle;
        this.processorName = processorName;
        this.processorParameters = processorParameters;
    }

    @Override
    public void submit() throws WorkflowException {
        try {
            submitImpl();
        } catch (IOException e) {
            throw new WorkflowException("Failed to submit Hadoop job: " + e.getMessage(), e);
        }
    }

    public String getOutputDir() {
        return outputDir;
    }

    private void submitImpl() throws IOException {

        Job job = new Job(getProcessingService().getJobClient().getConf(), this.productionId);
        Configuration configuration = job.getConfiguration();

        configuration.set("hadoop.job.ugi", "hadoop,hadoop");  // user hadoop owns the outputs
        configuration.set("mapred.map.tasks.speculative.execution", "false");
        configuration.set("mapred.reduce.tasks.speculative.execution", "false");
        configuration.set("mapred.child.java.opts", "-Xmx1024m");
        // For debugging uncomment following line:
        // configuration.set("mapred.child.java.opts", "-Xmx1024m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8009");

        configuration.set(ProcessingConfiguration.CALVALUS_IDENTIFIER, this.productionId);
        configuration.set(ProcessingConfiguration.CALVALUS_BUNDLE, processorBundle);
        configuration.set(ProcessingConfiguration.CALVALUS_INPUT, StringUtils.join(this.inputFiles, ","));
        configuration.set(ProcessingConfiguration.CALVALUS_OUTPUT, this.outputDir);
        configuration.set(ProcessingConfiguration.CALVALUS_L2_OPERATOR, this.processorName);
        configuration.set(ProcessingConfiguration.CALVALUS_L2_PARAMETER, this.processorParameters);

        // clear output directory
        final Path outputPath = new Path(this.outputDir);
        final FileSystem fileSystem = outputPath.getFileSystem(configuration);
        fileSystem.delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormatClass(ExecutablesInputFormat.class);
        job.setMapperClass(BeamOperatorMapper.class);
        job.setNumReduceTasks(0);

        CalvalusClasspath.configure(processorBundle, configuration);
        JobID jobId = submitJob(job);
        setJobId(jobId);
    }
}
