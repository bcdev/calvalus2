/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.ProcessingMetadata;
import com.bc.calvalus.processing.l2.L2FormattingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.snap.binning.TemporalBinSource;

import java.io.IOException;
import java.util.Map;

/**
 * the mapper for formatting the results of a SNAP Level 3 Hadoop Job.
 */
public class L3FormatterMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

    @Override
    public void run(Mapper.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        Path partsDirPath = fileSplit.getPath();

        Map<String, String> metadata = ProcessingMetadata.read(partsDirPath, conf);
        ProcessingMetadata.metadata2Config(metadata, conf, JobConfigNames.LEVEL3_METADATA_KEYS);

        final TemporalBinSource temporalBinSource = new L3TemporalBinSource(partsDirPath, context);

        String dateStart = conf.get(JobConfigNames.CALVALUS_MIN_DATE);
        String dateStop = conf.get(JobConfigNames.CALVALUS_MAX_DATE);
        String outputPrefix = conf.get(JobConfigNames.CALVALUS_OUTPUT_PREFIX, "L3");
        String outputPostfix = conf.get(JobConfigNames.CALVALUS_OUTPUT_POSTFIX, "");

        // todo - specify common Calvalus L3 productName convention (mz)
        String productName = String.format("%s_%s_%s%s", outputPrefix, dateStart, dateStop, outputPostfix);
        if (context.getConfiguration().get(JobConfigNames.CALVALUS_OUTPUT_REGEX) != null
                && context.getConfiguration().get(JobConfigNames.CALVALUS_OUTPUT_REPLACEMENT) != null) {
            productName = L2FormattingMapper.getProductName(context.getConfiguration(), productName);
        }

        L3Formatter.write(context, temporalBinSource,
                          dateStart, dateStop,
                          conf.get(JobConfigNames.CALVALUS_INPUT_REGION_NAME),
                          conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY),
                          productName);
    }
}
