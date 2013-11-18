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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 * Reads the records emitted by the MAMapper.
 * It is expected that each true 'record' key will only have one unique value.
 * Only 'header' keys ("#") will have multiple values containing all the same the attribute names.
 * This is why the reducer only writes the first value.
 *
 * @author Norman Fomferra
 */
public class MAReducer extends Reducer<Text, RecordWritable, Text, RecordWritable> {

    static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        final Configuration jobConfig = context.getConfiguration();
        final MAConfig maConfig = MAConfig.get(jobConfig);

        final PlotDatasetCollector plotDatasetCollector = new PlotDatasetCollector(maConfig.getOutputGroupName());

        final RecordProcessor[] recordProcessors = new RecordProcessor[]{
                new CsvRecordWriter(createWriter(context, "records-all.txt"),
                                    createWriter(context, "records-agg.txt"),
                                    createWriter(context, "annotated-records-all.txt"),
                                    createWriter(context, "annotated-records-agg.txt")),
                plotDatasetCollector,
        };

        LOG.warning("Collecting records...");
        int recordIndex = processRecords(context, recordProcessors);

        finalizeRecordProcessing(recordIndex, recordProcessors);

        final PlotDatasetCollector.PlotDataset[] plotDatasets = plotDatasetCollector.getPlotDatasets();
        ReportGenerator.generateReport(new TaskOutputStreamFactory(context),
                                       jobConfig,
                                       recordIndex,
                                       plotDatasets);
    }

    int processRecords(Context context, RecordProcessor[] recordProcessors) throws IOException, InterruptedException {
        int recordCount = 0;
        int exclusionIndex = -1;

        while (context.nextKey()) {
            final Text key = context.getCurrentKey();
            final Iterator<RecordWritable> iterator = context.getValues().iterator();
            if (iterator.hasNext()) {

                final RecordWritable record = iterator.next();
                context.write(key, record);

                if (key.equals(MAMapper.HEADER_KEY)) {
                    List<Object> annotNames = Arrays.asList(record.getAnnotationValues());
                    exclusionIndex = annotNames.indexOf(DefaultHeader.ANNOTATION_EXCLUSION_REASON);
                    processHeaderRecord(record, recordProcessors);
                } else {
                    processDataRecord(recordCount, record, recordProcessors);
                    if (exclusionIndex >= 0) {
                        String reason = (String) record.getAnnotationValues()[exclusionIndex];
                        if (!reason.isEmpty()) {
                            continue;
                        }
                    }
                    recordCount++;
                }
            }
        }
        return recordCount;
    }

    private void processHeaderRecord(RecordWritable record, RecordProcessor[] recordProcessors) throws IOException {
        for (RecordProcessor recordProcessor : recordProcessors) {
            recordProcessor.processHeaderRecord(record.getAttributeValues(), record.getAnnotationValues());
        }
    }

    private void processDataRecord(int recordIndex, RecordWritable record, RecordProcessor[] recordProcessors) throws IOException {
        for (RecordProcessor recordProcessor : recordProcessors) {
            recordProcessor.processDataRecord(recordIndex, record.getAttributeValues(), record.getAnnotationValues());
        }
    }

    private void finalizeRecordProcessing(int numRecords, RecordProcessor[] recordProcessors) throws IOException {
        for (RecordProcessor recordProcessor : recordProcessors) {
            recordProcessor.finalizeRecordProcessing(numRecords);
        }
    }

    public static Writer createWriter(Context context, String fileName) throws IOException, InterruptedException {
        return new OutputStreamWriter(TaskOutputStreamFactory.createOutputStream(context, fileName));
    }

}
