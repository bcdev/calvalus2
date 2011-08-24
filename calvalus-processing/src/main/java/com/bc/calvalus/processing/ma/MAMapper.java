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
import com.bc.calvalus.processing.beam.ProductFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.util.io.FileUtils;

import java.io.IOException;
import java.net.InetAddress;
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
        final long mapperStartTime = now();

        final Configuration jobConfig = context.getConfiguration();
        final MAConfig maConfig = MAConfig.fromXml(jobConfig.get(JobConfNames.CALVALUS_MA_PARAMETERS));
        final ProductFactory productFactory = new ProductFactory(jobConfig);

        final FileSplit split = (FileSplit) context.getInputSplit();

        // write initial log entry for runtime measurements
        LOG.info(String.format("%s starts processing of split %s (%s MiB)",
                               context.getTaskAttemptID(), split, (MiB / 2 + split.getLength()) / MiB));

        long t0;


        t0 = now();
        final Path inputPath = split.getPath();

        String inputFormat = jobConfig.get(JobConfNames.CALVALUS_INPUT_FORMAT, "ENVISAT");
        String regionGeometryWkt = jobConfig.get(JobConfNames.CALVALUS_REGION_GEOMETRY);
        String level2OperatorName = jobConfig.get(JobConfNames.CALVALUS_L2_OPERATOR);
        String level2Parameters = jobConfig.get(JobConfNames.CALVALUS_L2_PARAMETERS);
        Product product = productFactory.getProduct(inputPath,
                                                    inputFormat,
                                                    regionGeometryWkt,
                                                    level2OperatorName,
                                                    level2Parameters);
        // Actually wrong name for processed products, but we need the field "source_name" in the export data table
        product.setName(FileUtils.getFilenameWithoutExtension(inputPath.getName()));

        context.progress();
        long productOpenTime = (now() - t0);
        LOG.info(String.format("%s opened product %s, took %s sec",
                               context.getTaskAttemptID(), product.getName(), productOpenTime / 1E3));

        t0 = now();
        Extractor extractor = new Extractor(product, maConfig);
        Iterable<Record> extractedRecords = getExtractedRecords(extractor, maConfig);
        context.progress();
        long recordReadTime = (now() - t0);
        LOG.info(String.format("%s read input records from %s, took %s sec",
                               context.getTaskAttemptID(), maConfig.getRecordSourceUrl(), recordReadTime / 1E3));

        RecordTransformer recordTransformer = new RecordTransformer(-1, 1.5);

        t0 = now();
        int numMatchUps = 0;
        int numEmittedRecords = 0;
        for (Record extractedRecord : extractedRecords) {

            // write records
            if (maConfig.isAggregateMacroPixel()) {
                Record aggregatedRecord = recordTransformer.aggregate(extractedRecord);
                context.write(new Text(String.format("%s_%06d", product.getName(), numEmittedRecords + 1)),
                              new RecordWritable(aggregatedRecord.getAttributeValues(), extractor.getHeader().getTimeFormat()));
                numEmittedRecords++;
            } else {
                for (Record expandedRecord : recordTransformer.expand(extractedRecord)) {
                    context.write(new Text(String.format("%s_%06d", product.getName(), numEmittedRecords + 1)),
                                  new RecordWritable(expandedRecord.getAttributeValues(), extractor.getHeader().getTimeFormat()));
                    numEmittedRecords++;
                }
            }

            numMatchUps++;
            context.progress();
        }
        long recordWriteTime = (now() - t0);
        LOG.info(String.format("%s found %s match-ups, emitted %s records, took %s sec",
                               context.getTaskAttemptID(), numMatchUps, numEmittedRecords, recordWriteTime / 1E3));

        if (numMatchUps > 0) {
            // write header
            context.write(new Text("#"),
                          new RecordWritable(extractor.getHeader().getAttributeNames()));
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Products with match-ups").increment(1);
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Number of match-ups").increment(numMatchUps);
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Number of records").increment(numEmittedRecords);
        } else {
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Products without match-ups").increment(1);
        }


        t0 = now();
        product.dispose();
        context.progress();
        long productCloseTime = (now() - t0);
        LOG.info(String.format("%s closed input product, took %s sec",
                               context.getTaskAttemptID(), productCloseTime / 1E3));

        // write final log entry for runtime measurements
        long mapperTotalTime = (now() - mapperStartTime);
        LOG.info(String.format("%s stops processing of split %s after %s sec",
                               context.getTaskAttemptID(), split, mapperTotalTime / 1E3));

        // per-host counters
        String hostGroupName = "MAMapper on " + InetAddress.getLocalHost().getHostName();
        context.getCounter(hostGroupName, "MAMapper runs").increment(1);
        context.getCounter(hostGroupName, "Product open time (ms)").increment(productOpenTime);
        context.getCounter(hostGroupName, "Record read time (ms)").increment(recordReadTime);
        context.getCounter(hostGroupName, "Record write time (ms)").increment(recordWriteTime);
        context.getCounter(hostGroupName, "Product close time (ms)").increment(productCloseTime);
        context.getCounter(hostGroupName, "MAMapper total time (ms)").increment(mapperTotalTime);

    }

    private static long now() {
        return System.currentTimeMillis();
    }

    private static Iterable<Record> getExtractedRecords(Extractor extractor, MAConfig maConfig) {
        Iterable<Record> extractedRecords;
        try {
            final RecordSource recordSource = maConfig.createRecordSource();
            extractor.setInput(recordSource);
            extractor.setSortInputByPixelYX(true);
            extractedRecords = extractor.getRecords();
        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve input records.", e);
        }
        return extractedRecords;
    }
}
