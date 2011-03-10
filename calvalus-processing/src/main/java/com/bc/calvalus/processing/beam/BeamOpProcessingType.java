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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Creates a beam hadoop job
 */
public class BeamOpProcessingType {

    private JobClient jobClient;

    public BeamOpProcessingType(JobClient jobClient) {
        this.jobClient = jobClient;
    }

    public Job createBeamHadoopJob(String wpsXmlRequest) throws Exception {
        WpsConfig wpsConfig = new WpsConfig(wpsXmlRequest);
        String requestOutputDir = wpsConfig.getRequestOutputDir();
        String identifier = wpsConfig.getIdentifier();

        // construct job and set parameters and handlers
        Job job = new Job(jobClient.getConf(), identifier);
        Configuration configuration = job.getConfiguration();
        ProcessingConfiguration processingConfiguration = new ProcessingConfiguration(configuration);
        processingConfiguration.addWpsParameters(wpsConfig);

        // clear output directory
        final Path outputPath = new Path(requestOutputDir);
        final FileSystem fileSystem = outputPath.getFileSystem(configuration);
        fileSystem.delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormatClass(ExecutablesInputFormat.class);

        if (processingConfiguration.getLevel3Parameters() != null) {
            job.setNumReduceTasks(16);

            job.setMapperClass(L3Mapper.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(SpatialBin.class);

            job.setPartitionerClass(L3Partitioner.class);

            job.setReducerClass(L3Reducer.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(TemporalBin.class);

            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            // todo - scan all input paths, collect all products and compute min start/ max stop sensing time
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

        return job;
    }

    public JobID submitJob(String wpsXml) throws Exception {
        Job job = createBeamHadoopJob(wpsXml);
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
