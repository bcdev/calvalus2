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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.esa.beam.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;

import static com.bc.calvalus.processing.hadoop.HadoopProcessingService.*;

/**
 * Creates a beam hadoop job
 */
public class BeamOpProcessingType {

    private JobClient jobClient;

    public BeamOpProcessingType(JobClient jobClient) {
        this.jobClient = jobClient;
    }

    public Job createJobFromWps(String wpsXmlRequest) throws Exception {
        WpsConfig wpsConfig = new WpsConfig(wpsXmlRequest);
        String identifier = wpsConfig.getIdentifier();
        Job job = new Job(jobClient.getConf(), identifier);
        Configuration conf = job.getConfiguration();
        addIfNotEmpty(conf, JobConfNames.CALVALUS_IDENTIFIER, wpsConfig.getIdentifier());
        addIfNotEmpty(conf, JobConfNames.CALVALUS_BUNDLE, wpsConfig.getProcessorPackage());
        String[] requestInputPaths = wpsConfig.getRequestInputPaths();
        String inputs = StringUtils.join(requestInputPaths, ",");
        addIfNotEmpty(conf, JobConfNames.CALVALUS_INPUT, inputs);
        addIfNotEmpty(conf, JobConfNames.CALVALUS_OUTPUT, wpsConfig.getRequestOutputDir());
        addIfNotEmpty(conf, JobConfNames.CALVALUS_ROI_WKT, wpsConfig.getRoiWkt());
        addIfNotEmpty(conf, JobConfNames.CALVALUS_L2_OPERATOR, wpsConfig.getOperatorName());
        addIfNotEmpty(conf, JobConfNames.CALVALUS_L2_PARAMETER, wpsConfig.getLevel2Paramter());
        addIfNotEmpty(conf, JobConfNames.CALVALUS_L3_PARAMETER, wpsConfig.getLevel3Paramter());
        addIfNotEmpty(conf, JobConfNames.CALVALUS_FORMATTER_PARAMETER, wpsConfig.getFormatterParameter());
        return job;
    }

    public static void addIfNotEmpty(Configuration conf, String key, String value) {
        if (value != null && !value.isEmpty()) {
            conf.set(key, value);
        }
    }

    public void configureJob(Job job) throws IOException {
        Configuration configuration = job.getConfiguration();
        String output = configuration.get(JobConfNames.CALVALUS_OUTPUT);
        // clear output directory
        final Path outputPath = new Path(output);
        final FileSystem fileSystem = outputPath.getFileSystem(configuration);
        fileSystem.delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormatClass(CalvalusInputFormat.class);

        if (configuration.get(JobConfNames.CALVALUS_L3_PARAMETER) != null) {
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

        addBundleToClassPath(BEAM_BUNDLE, configuration);
        addBundleToClassPath(configuration.get(JobConfNames.CALVALUS_BUNDLE), configuration);
    }
}
