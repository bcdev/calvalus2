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
import java.util.ArrayList;
import java.util.Map;

/**
 * Creates a beam hadoop job
 */
public class BeamOpProcessingType {

    private JobClient jobClient;

    public BeamOpProcessingType(JobClient jobClient) {
        this.jobClient = jobClient;
    }

    public JobID submitJob(Map<String, Object> parameters) throws Exception {
        Job job = createJobFromParameters(parameters);
        configureJob(job);
        return submitJobImpl(job);
    }

    public Job createJobFromWps(String wpsXmlRequest) throws Exception {
        WpsConfig wpsConfig = new WpsConfig(wpsXmlRequest);
        String identifier = wpsConfig.getIdentifier();
        Job job = new Job(jobClient.getConf(), identifier);
        Configuration conf = job.getConfiguration();
        addIfNotEmpty(conf, ProcessingConfiguration.CALVALUS_IDENTIFIER, wpsConfig.getIdentifier());
        addIfNotEmpty(conf, ProcessingConfiguration.CALVALUS_BUNDLE, wpsConfig.getProcessorPackage());
        String[] requestInputPaths = wpsConfig.getRequestInputPaths();
        String inputs = StringUtils.join(requestInputPaths, ",");
        addIfNotEmpty(conf, ProcessingConfiguration.CALVALUS_INPUT, inputs);
        addIfNotEmpty(conf, ProcessingConfiguration.CALVALUS_OUTPUT, wpsConfig.getRequestOutputDir());
        addIfNotEmpty(conf, ProcessingConfiguration.CALVALUS_L2_OPERATOR, wpsConfig.getOperatorName());
        addIfNotEmpty(conf, ProcessingConfiguration.CALVALUS_L2_PARAMETER, wpsConfig.getLevel2Paramter());
        addIfNotEmpty(conf, ProcessingConfiguration.CALVALUS_L3_PARAMETER, wpsConfig.getLevel3Paramter());
        addIfNotEmpty(conf, ProcessingConfiguration.CALVALUS_FORMATTER_PARAMETER, wpsConfig.getFormatterParameter());
        return job;
    }

    private static void addIfNotEmpty(Configuration conf, String key, String value) {
        if (value != null && !value.isEmpty()) {
            conf.set(key, value);
        }
    }

    private Job createJobFromParameters(Map<String, Object> parameters) throws IOException {
        String productionId = getString(parameters, "productionId");

        Job job = new Job(jobClient.getConf(), productionId);
        Configuration configuration = job.getConfiguration();

        //TODO generalise this (copy everything from parameters map?)
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
        Object level3Parameters = parameters.get("binningParameters");
        if (level3Parameters instanceof L3Config) {
            L3Config l3Config = (L3Config) level3Parameters;
            String l3Params = BeamUtils.saveAsXml(l3Config);
            configuration.set(ProcessingConfiguration.CALVALUS_L3_PARAMETER, l3Params);
        }
        return job;
    }

    private static String getString(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            throw new IllegalArgumentException(String.format("Missing parameter '%s'.", key));
        }
        if (!(value instanceof String)) {
            throw new IllegalArgumentException(String.format("Parameter '%s' must be a String.", key));
        }
        return (String) value;
    }

    public void configureJob(Job job) throws IOException {
        Configuration configuration = job.getConfiguration();
        String output = configuration.get(ProcessingConfiguration.CALVALUS_OUTPUT);
        // clear output directory
        final Path outputPath = new Path(output);
        final FileSystem fileSystem = outputPath.getFileSystem(configuration);
        fileSystem.delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormatClass(ExecutablesInputFormat.class);

        if (configuration.get(ProcessingConfiguration.CALVALUS_L3_PARAMETER) != null) {
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

        CalvalusClasspath.configure(configuration.get(ProcessingConfiguration.CALVALUS_BUNDLE), configuration);
    }


    private JobID submitJobImpl(Job job) throws Exception {
        Configuration configuration = job.getConfiguration();
        //add calvalus itself to classpath of hadoop jobs
        CalvalusClasspath.addPackageToClassPath("calvalus-1.0-SNAPSHOT", configuration);
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
