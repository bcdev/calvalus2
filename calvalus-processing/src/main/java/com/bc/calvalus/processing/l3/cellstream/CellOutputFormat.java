/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.l3.cellstream;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.l3.L3TemporalBin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * An {@link org.apache.hadoop.mapreduce.OutputFormat} for writing Cells (aka {@link com.bc.calvalus.processing.l3.L3TemporalBin TemporalBins})
 * to {@link org.apache.hadoop.io.SequenceFile}s.
 */
public class CellOutputFormat extends FileOutputFormat<LongWritable, L3TemporalBin> {

    @Override
    public RecordWriter<LongWritable, L3TemporalBin> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        CompressionCodec codec = null;
        SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.NONE;
        if (getCompressOutput(context)) {
            // find the kind of compression to do
            compressionType = SequenceFileOutputFormat.getOutputCompressionType(context);

            // find the right codec
            Class<?> codecClass = getOutputCompressorClass(context, DefaultCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        }
        // get the path of the temporary output file
        Path file = getDefaultWorkFile(context, "");
        FileSystem fs = file.getFileSystem(conf);

        SequenceFile.Metadata metadata = new SequenceFile.Metadata();
        Conf2MetaCopier conf2Meta = new Conf2MetaCopier(conf, metadata);
        conf2Meta.copy("calvalus.l3.outputFeatureNames", "calvalus.l3.featureNames");
        conf2Meta.copy(JobConfigNames.CALVALUS_MIN_DATE);
        conf2Meta.copy(JobConfigNames.CALVALUS_MAX_DATE);

        // TODO add l3_params ???
        final SequenceFile.Writer out = SequenceFile.createWriter(fs, conf, file,
                                                                  context.getOutputKeyClass(),
                                                                  context.getOutputValueClass(),
                                                                  compressionType,
                                                                  codec,
                                                                  context,
                                                                  metadata);

        return new RecordWriter<LongWritable, L3TemporalBin>() {

            public void write(LongWritable key, L3TemporalBin value) throws IOException {
                out.append(key, value);
            }

            public void close(TaskAttemptContext context) throws IOException {
                out.close();
            }
        };
    }


    private static class Conf2MetaCopier {
        private final Configuration configuration;
        private final SequenceFile.Metadata metadata;

        private Conf2MetaCopier(Configuration configuration, SequenceFile.Metadata metadata) {
            this.configuration = configuration;
            this.metadata = metadata;
        }

        private void copy(String propertyName) {
            copy(propertyName, propertyName);
        }

        private void copy(String configurationPropertyName, String metadataPropertyName) {
            String value = configuration.get(configurationPropertyName);
            if (value != null) {
                metadata.set(new Text(metadataPropertyName), new Text(value));
            }
        }
    }
}
