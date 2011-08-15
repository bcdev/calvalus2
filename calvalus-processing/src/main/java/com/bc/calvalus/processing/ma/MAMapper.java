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

package com.bc.calvalus.processing.ma;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfNames;
import com.bc.calvalus.processing.beam.BeamUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.util.io.FileUtils;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Reads an N1 product and emits records (binIndex, spatialBin).
 *
 * @author Norman Fomferra
 */
public class MAMapper extends Mapper<NullWritable, NullWritable, Text, RecordWritable> {

    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";
    private static final Logger LOG = CalvalusLogger.getLogger();
    public static final int MiB = 1024 * 1024;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        BeamUtils.initGpf(configuration);
        final MAConfig maConfig = MAConfig.fromXml(configuration.get(JobConfNames.CALVALUS_MA_PARAMETERS));

        final FileSplit split = (FileSplit) context.getInputSplit();

        // write initial log entry for runtime measurements
        LOG.info(String.format("%s starts processing of split %s (%s MiB)",
                               context.getTaskAttemptID(), split, (MiB/2 + split.getLength()) / MiB));

        final long startTime = System.nanoTime();

        final Path inputPath = split.getPath();
        String inputName = FileUtils.getFilenameWithoutExtension(inputPath.getName());

        final Product product = BeamUtils.readProduct(inputPath, configuration);
        product.setName(inputName);

        LOG.info(String.format("%s opened product %s, took %s sec",
                               context.getTaskAttemptID(), inputName, (System.nanoTime() - startTime) / 1E9));

        Extractor extractor = new Extractor(product);
        Iterable<Record> extractedRecords;
        try {
            final RecordSource recordSource = maConfig.createRecordSource();
            extractor.setInput(recordSource);
            extractor.setCopyInput(true);
            extractor.setSortInputByPixelYX(true);
            if (maConfig.getExportDateFormat() != null) {
                extractor.setDateFormat(maConfig.getExportDateFormat());
            }
            extractedRecords = extractor.getRecords();
        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve input records.", e);
        }

        int numMatchUps = 0;
        for (Record extractedRecord : extractedRecords) {
            // write record
            context.write(new Text(String.format("%s_%06d", product.getName(), numMatchUps + 1)),
                          new RecordWritable(extractedRecord));
            numMatchUps++;
        }

        LOG.info(String.format("%s extracted %s match-ups, took %s sec so far",
                               context.getTaskAttemptID(), numMatchUps, (System.nanoTime() - startTime) / 1E9));

        if (numMatchUps > 0) {
            // write header
            context.write(new Text("#"),
                          new RecordWritable(extractor.getHeader().getAttributeNames()));
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Products with match-ups").increment(1);
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Number of match-ups total").increment(numMatchUps);
        } else {
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Products without match-ups").increment(1);
        }

        product.dispose();

        // write final log entry for runtime measurements
        LOG.info(String.format("%s stops processing of split %s after %s sec",
                               context.getTaskAttemptID(), split, (System.nanoTime() - startTime) / 1E9));
    }
}
