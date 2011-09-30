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
import com.bc.calvalus.processing.hadoop.MultiFileSingleBlockInputFormat;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.WpsConfig;
import com.bc.calvalus.processing.l3.L3Mapper;
import com.bc.calvalus.processing.l3.L3Partitioner;
import com.bc.calvalus.processing.l3.L3Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.esa.beam.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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

        String[] requestInputPaths = wpsConfig.getRequestInputPaths();
        String filenamePattern = wpsConfig.getFilenamePattern();
        String inputs = collectInputPaths(requestInputPaths, filenamePattern, conf);
        String priority = wpsConfig.getPriority();
        if (priority == null) {
            priority = "LOW";
        }
        addIfNotEmpty(conf, JobConfigNames.CALVALUS_INPUT, inputs);
        addIfNotEmpty(conf, JobConfigNames.CALVALUS_INPUT_FORMAT, wpsConfig.getInputFormat());
        addIfNotEmpty(conf, JobConfigNames.CALVALUS_OUTPUT_DIR, wpsConfig.getRequestOutputDir());
        addIfNotEmpty(conf, JobConfigNames.CALVALUS_L2_BUNDLE, wpsConfig.getProcessorPackage());
        addIfNotEmpty(conf, JobConfigNames.CALVALUS_L2_OPERATOR, wpsConfig.getOperatorName());
        addIfNotEmpty(conf, JobConfigNames.CALVALUS_L2_PARAMETERS, wpsConfig.getLevel2Parameters());
        addIfNotEmpty(conf, JobConfigNames.CALVALUS_L3_PARAMETERS, wpsConfig.getLevel3Parameters());
        addIfNotEmpty(conf, JobConfigNames.CALVALUS_REGION_GEOMETRY, wpsConfig.getGeometry());
        addIfNotEmpty(conf, JobConfigNames.CALVALUS_FORMATTER_PARAMETERS, wpsConfig.getFormatterParameters());
        addIfNotEmpty(conf, JobConfigNames.CALVALUS_PRIORITY, priority);

        Map<String,String> propertiesMap = wpsConfig.getSystemProperties();
        if (!propertiesMap.containsKey("beam.reader.tileHeight")) {
            propertiesMap.put("beam.reader.tileHeight", "64");
        }
        if (!propertiesMap.containsKey("beam.reader.tileWidth")) {
            propertiesMap.put("beam.reader.tileWidth", "*");
        }
        for (Map.Entry<String, String> propertiesEntry : propertiesMap.entrySet()) {
            String key = propertiesEntry.getKey();
            String value = propertiesEntry.getValue();
            conf.set("calvalus.system." + key, value);
        }
        return job;
    }

    public static String collectInputPaths(String[] requestInputPaths, String filenamePattern, Configuration conf) throws IOException {
        Pattern filter = (filenamePattern != null) ? Pattern.compile(filenamePattern) : null;
        List<String> collectedInputPaths = new ArrayList<String>();
        for (String inputUrl : requestInputPaths) {
            Path input = new Path(inputUrl);
            FileSystem fs = input.getFileSystem(conf);
            FileStatus[] fileStatuses = fs.listStatus(input);
            collectedInputPaths(fileStatuses, fs, filter, collectedInputPaths);
        }
        return StringUtils.join(collectedInputPaths, ",");
    }

    private static void collectedInputPaths(FileStatus[] fileStatuses, FileSystem fs, Pattern filter, List<String> collectedInputPaths) throws IOException {
        for (FileStatus fileStatus : fileStatuses) {
            final Path path = fileStatus.getPath();
            if (fileStatus.isDir()) {
                FileStatus[] subdir = fs.listStatus(path);
                collectedInputPaths(subdir, fs, filter, collectedInputPaths);
            } else {
                if (filter == null || filter.matcher(path.getName()).matches()) {
                    collectedInputPaths.add(path.toString());
                }
            }
        }
    }

    public static void addIfNotEmpty(Configuration conf, String key, String value) {
        if (value != null && !value.isEmpty()) {
            conf.set(key, value);
        }
    }

    public void configureJob(Job job) throws IOException {
        Configuration configuration = job.getConfiguration();
        String output = configuration.get(JobConfigNames.CALVALUS_OUTPUT_DIR);
        // clear output directory
        final Path outputPath = new Path(output);
        final FileSystem fileSystem = outputPath.getFileSystem(configuration);
        fileSystem.delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormatClass(MultiFileSingleBlockInputFormat.class);

        if (configuration.get(JobConfigNames.CALVALUS_L3_PARAMETERS) != null) {
            job.setNumReduceTasks(4);

            job.setMapperClass(L3Mapper.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(SpatialBin.class);

            job.setPartitionerClass(L3Partitioner.class);

            job.setReducerClass(L3Reducer.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(TemporalBin.class);

            job.setOutputFormatClass(SequenceFileOutputFormat.class);

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
        configuration.set("mapred.child.java.opts", "-Xmx2000m");
        job.getConfiguration().setInt("mapred.max.map.failures.percent", 20);

        addBundleToClassPath(configuration.get(JobConfigNames.CALVALUS_BEAM_BUNDLE, DEFAULT_BEAM_BUNDLE), configuration);
        addBundleToClassPath(configuration.get(JobConfigNames.CALVALUS_L2_BUNDLE), configuration);
    }
}
