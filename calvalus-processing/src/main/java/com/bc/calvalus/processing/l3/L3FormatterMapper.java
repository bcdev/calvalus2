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

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.ProcessingMetadata;
import com.bc.calvalus.processing.l2.ProductFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.binning.TemporalBinSource;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * the mapper for formatting the results of a BEAM Level 3 Hadoop Job.
 */
public class L3FormatterMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {
    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";
    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public void run(Mapper.Context context) throws IOException, InterruptedException {
        Configuration jobConfig = context.getConfiguration();
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        Path partsDirPath = fileSplit.getPath();

        Map<String, String> metadata = ProcessingMetadata.read(partsDirPath, jobConfig);
        ProcessingMetadata.metadata2Config(metadata, jobConfig, JobConfigNames.LEVEL3_METADATA_KEYS);

        final TemporalBinSource temporalBinSource = new L3TemporalBinSource(partsDirPath, context);

        String dateStart = jobConfig.get(JobConfigNames.CALVALUS_MIN_DATE);
        String dateStop = jobConfig.get(JobConfigNames.CALVALUS_MAX_DATE);
        String outputPrefix = jobConfig.get(JobConfigNames.CALVALUS_OUTPUT_PREFIX, "L3");

        // todo - specify common Calvalus L3 productName convention (mz)
        String productName = String.format("%s_%s_%s", outputPrefix, dateStart, dateStop);

        String format = jobConfig.get(JobConfigNames.CALVALUS_OUTPUT_FORMAT, null);
        String compression = jobConfig.get(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, null);
        ProductFormatter productFormatter = new ProductFormatter(productName, format, compression);
        try {
            File productFile = productFormatter.createTemporaryProductFile();

            L3Formatter formatter = new L3Formatter(dateStart, dateStop,
                                                    productFile.getAbsolutePath(),
                                                    productFormatter.getOutputFormat(),
                                                    jobConfig);
            LOG.info("Start formatting product to file: " + productFile.getName());
            context.setStatus("formatting");
            formatter.format(temporalBinSource,
                    jobConfig.get(JobConfigNames.CALVALUS_INPUT_REGION_NAME),
                    jobConfig.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));

            LOG.info("Finished formatting product.");
            context.setStatus("copying");
            productFormatter.compressToHDFS(context, productFile);
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product formatted").increment(1);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Formatting failed.", e);
            throw new IOException(e);
        } finally {
            productFormatter.cleanupTempDir();
            context.setStatus("");
        }
    }
}
