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

import com.bc.calvalus.binning.SpatialBin;
import com.bc.calvalus.binning.TemporalBin;
import com.bc.calvalus.processing.shellexec.ExecutablesInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.esa.beam.util.StringUtils;

import java.io.IOException;
import java.util.Map;

import java.util.ArrayList;

/**
 * Creates a beam hadoop job
 */
public class BeamOpProcessingType {

    private JobClient jobClient;

    public BeamOpProcessingType(JobClient jobClient) {
        this.jobClient = jobClient;
    }

    public JobID submitJob(String wpsXml) throws Exception {
        Job job = createJob(wpsXml);
        configureJob(job);
        return submitJobImpl(job);
    }

    public JobID submitJob(Map<String, Object> parameters) throws Exception {
        Job job = createJob(parameters);
        configureJob(job);
        return submitJobImpl(job);
    }

    public Job createJob(String wpsXmlRequest) throws Exception {
        WpsConfig wpsConfig = new WpsConfig(wpsXmlRequest);
        String identifier = wpsConfig.getIdentifier();
        Job job = new Job(jobClient.getConf(), identifier);
        ProcessingConfiguration processingConfiguration = new ProcessingConfiguration(job.getConfiguration());
        processingConfiguration.addWpsParameters(wpsConfig);
        return job;
    }

    // at the moment only for beam-op-level 2
    private Job createJob(Map<String, Object> parameters) throws IOException {
        String productionId = getString(parameters, "productionId");

        Job job = new Job(jobClient.getConf(), productionId);
        Configuration configuration = job.getConfiguration();

        configuration.set(ProcessingConfiguration.CALVALUS_IDENTIFIER, productionId);
        String name = getString(parameters, "processorBundleName");
        String version = getString(parameters, "processorBundleVersion");
        configuration.set(ProcessingConfiguration.CALVALUS_BUNDLE, name + "-" + version);
        String[] inputFiles = (String[]) parameters.get("inputFiles");
        String inputs = StringUtils.join(inputFiles, ",");
        configuration.set(ProcessingConfiguration.CALVALUS_INPUT, inputs);
        configuration.set(ProcessingConfiguration.CALVALUS_OUTPUT, getString(parameters, "outputDir"));
        configuration.set(ProcessingConfiguration.CALVALUS_L2_OPERATOR, getString(parameters, "processorName"));
        configuration.set(ProcessingConfiguration.CALVALUS_L2_PARAMETER, getString(parameters, "processorParameters"));
        return job;
    }

    private static String getString(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof String) {
            return (String) value;
        } else {
            throw new IllegalArgumentException(String.format("The parameters '%s' is not a String.", key));
        }
    }

    public void configureJob(Job job) throws IOException {
        Configuration configuration = job.getConfiguration();
        ProcessingConfiguration processingConfiguration = new ProcessingConfiguration(configuration);
        // clear output directory
        final Path outputPath = new Path(processingConfiguration.getOutputPath());
        final FileSystem fileSystem = outputPath.getFileSystem(configuration);
        fileSystem.delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormatClass(ExecutablesInputFormat.class);

        if (processingConfiguration.getLevel3Parameters() != null) {
            job.setNumReduceTasks(4);

            job.setMapperClass(L3Mapper.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(SpatialBin.class);

            job.setPartitionerClass(L3Partitioner.class);

            job.setReducerClass(L3Reducer.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(TemporalBin.class);

            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            if (configuration.getBoolean("calvalus.l3ta", false)) {

                // todo - this doesn't work! Grrrr!
                Job job2 = new Job(jobClient.getConf(), "calvalus.l3ta");
                job2.setNumReduceTasks(4);

                job2.setMapperClass(L3Mapper.class);
                job2.setMapOutputKeyClass(LongWritable.class);
                job2.setMapOutputValueClass(SpatialBin.class);

                job2.setPartitionerClass(L3Partitioner.class);

                job2.setReducerClass(L3Reducer.class);
                job2.setOutputKeyClass(LongWritable.class);
                job2.setOutputValueClass(TemporalBin.class);

                job2.setOutputFormatClass(SequenceFileOutputFormat.class);

                org.apache.hadoop.mapred.jobcontrol.Job jc1 = new org.apache.hadoop.mapred.jobcontrol.Job((JobConf) job.getConfiguration());
                ArrayList<org.apache.hadoop.mapred.jobcontrol.Job> dependingJobs = new ArrayList<org.apache.hadoop.mapred.jobcontrol.Job>();
                dependingJobs.add(jc1);
                JobControl jobControl = new JobControl("calvalus.l3ta");
                org.apache.hadoop.mapred.jobcontrol.Job jc2 = new org.apache.hadoop.mapred.jobcontrol.Job((JobConf) job2.getConfiguration(),
                                                                                                          dependingJobs);
                jobControl.addJob(jc1);
                jobControl.addJob(jc2);
                jobControl.run();

            }


        } else {
            job.setMapperClass(BeamOperatorMapper.class);
            job.setNumReduceTasks(0);
            //job.setOutputFormatClass(TextOutputFormat.class);
            //job.setOutputKeyClass(Text.class);
            //job.setOutputValueClass(Text.class);
        }
        configuration.set("hadoop.job.ugi", "hadoop,hadoop");  // user hadoop owns the outputs
        configuration.set("mapred.map.tasks.speculative.execution", "false");
        configuration.set("mapred.reduce.tasks.speculative.execution", "false");
        //conf.set("mapred.child.java.opts", "-Xmx1024m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8009");
        configuration.set("mapred.child.java.opts", "-Xmx1024m");

        BeamCalvalusClasspath.configure(processingConfiguration.getProcessorBundle(), configuration);
    }


    private JobID submitJobImpl(Job job) throws Exception {
        Configuration configuration = job.getConfiguration();
        //add calvalus itself to classpath of hadoop jobs
        BeamCalvalusClasspath.addPackageToClassPath("calvalus-1.0-SNAPSHOT", configuration);
        JobConf jobConf;
        if (configuration instanceof JobConf) {
            jobConf = (JobConf) configuration;
        } else {
            jobConf = new JobConf(configuration);
        }
        jobConf.setUseNewMapper(true);
        jobConf.setUseNewReducer(true);
        RunningJob runningJob = jobClient.submitJob(jobConf);
        return runningJob.getID();
    }


}
