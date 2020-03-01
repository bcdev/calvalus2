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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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

        final PlotDatasetCollector plotDatasetCollector = new PlotDatasetCollector(maConfig.getOutputGroupName(),
                                                                                   maConfig.getVariableMappings());

        final RecordProcessor[] recordProcessors = new RecordProcessor[]{
                new CsvRecordWriter(createWriter(context, "records-all.txt"),
                                    createWriter(context, "records-agg.txt"),
                                    createWriter(context, "annotated-records-all.txt"),
                                    createWriter(context, "annotated-records-agg.txt")),
                plotDatasetCollector,
        };

        LOG.warning("Collecting records...");
        Map<String, Integer> annotatedRecordCounts = processRecords(context, recordProcessors);

        // moved to processRecords: finalizeRecordProcessing(recordProcessors);

        final PlotDatasetCollector.PlotDataset[] plotDatasets = plotDatasetCollector.getPlotDatasets();
        ReportGenerator.generateReport(new TaskOutputStreamFactory(context),
                                       jobConfig,
                                       annotatedRecordCounts,
                                       plotDatasets);
    }

    Map<String, Integer> processRecords(Context context, RecordProcessor[] recordProcessors) throws IOException, InterruptedException {
        Map<String, Integer> exclusionRecordCounts = new HashMap<String, Integer>();
        int goodRecordCount = 0;
        int totalRecordCount = 0;
        int exclusionIndex = -1;
        List<String> keyFilenameStack = new ArrayList<>();
        List<RecordProcessor[]> recordProcessorStack = new ArrayList<>();
        String previousHeader = null;

        while (context.nextKey()) {
            final Text key = context.getCurrentKey();
            final Iterator<RecordWritable> iterator = context.getValues().iterator();
            if (iterator.hasNext()) {

                final RecordWritable record = iterator.next();
                context.write(key, record);

                if (key.toString().startsWith("#_")) {
                    String thisHeader = Arrays.toString(record.getAttributeValues());
                    if (! thisHeader.equals(previousHeader)) {
                        final String keyFilename = key.toString().substring(2);
                        if (previousHeader == null) {
                            keyFilenameStack.add(0, keyFilename);
                            recordProcessorStack.add(0, recordProcessors);
                        } else {
                            LOG.info("table structure changes starting with " + keyFilename);
                            recordProcessors = createRecordProcessors(keyFilename, context, recordProcessors);
                            keyFilenameStack.add(0, keyFilename);
                            recordProcessorStack.add(0, recordProcessors);
                        }
                        previousHeader = thisHeader;
                        List<Object> annotNames = Arrays.asList(record.getAnnotationValues());
                        exclusionIndex = annotNames.indexOf(DefaultHeader.ANNOTATION_EXCLUSION_REASON);
                        processHeaderRecord(record, recordProcessors);
                    }
                } else {
                    final String keyFilename = key.toString().substring(key.toString().indexOf('_')+1);
                    recordProcessors = getRecordProcessors(keyFilename, keyFilenameStack, recordProcessorStack);
                    processDataRecord(key.toString(), record, recordProcessors);
                    totalRecordCount++;
                    if (exclusionIndex >= 0) {
                        String reason = (String) record.getAnnotationValues()[exclusionIndex];
                        if (reason.isEmpty()) {
                            goodRecordCount++;
                        } else {
                            Integer reasonCounter = exclusionRecordCounts.get(reason);
                            if (reasonCounter == null) {
                                reasonCounter = 1;
                            } else {
                                reasonCounter++;
                            }
                            exclusionRecordCounts.put(reason, reasonCounter);
                        }
                    }
                }
            }
        }
        for (RecordProcessor[] processors : recordProcessorStack) {
             finalizeRecordProcessing(processors);
        }

        Map<String, Integer> annotatedRecordCounts = new LinkedHashMap<String, Integer>();
        annotatedRecordCounts.put("Total", totalRecordCount);
        ArrayList<String> keyList = new ArrayList<String>(exclusionRecordCounts.keySet());
        Collections.sort(keyList);
        for (String key : keyList) {
            annotatedRecordCounts.put(String.format("Excluded (%s)", key), exclusionRecordCounts.get(key));
        }
        annotatedRecordCounts.put("Good", goodRecordCount);
        return annotatedRecordCounts;
    }

    private RecordProcessor[] getRecordProcessors(String keyFilename,
                                                  List<String> keyFilenameStack,
                                                  List<RecordProcessor[]> recordProcessorStack) {
        for (int i=0; i<keyFilenameStack.size(); ++i) {
            if (keyFilename.compareTo(keyFilenameStack.get(i)) >= 0) {
                return recordProcessorStack.get(i);
            }
        }
        throw new NoSuchElementException("cannot find writers for " + keyFilename);
    }

    private RecordProcessor[] createRecordProcessors(String keyFilename, Context context, RecordProcessor[] recordProcessors) throws IOException, InterruptedException {
        return new RecordProcessor[] {
                new CsvRecordWriter(createWriter(context, "records-all.starting-with-" + keyFilename + ".txt"),
                                    createWriter(context, "records-agg.starting-with-" + keyFilename + ".txt"),
                                    createWriter(context, "annotated-records-all.starting-with-" + keyFilename + ".txt"),
                                    createWriter(context, "annotated-records-agg.starting-with-" + keyFilename + ".txt")),
                recordProcessors[1]
        };
    }

    private void processHeaderRecord(RecordWritable record, RecordProcessor[] recordProcessors) throws IOException {
        for (RecordProcessor recordProcessor : recordProcessors) {
            recordProcessor.processHeaderRecord(record.getAttributeValues(), record.getAnnotationValues());
        }
    }

    private void processDataRecord(String key, RecordWritable record, RecordProcessor[] recordProcessors) throws IOException {
        for (RecordProcessor recordProcessor : recordProcessors) {
            recordProcessor.processDataRecord(key, record.getAttributeValues(), record.getAnnotationValues());
        }
    }

    private void finalizeRecordProcessing(RecordProcessor[] recordProcessors) throws IOException {
        for (RecordProcessor recordProcessor : recordProcessors) {
            recordProcessor.finalizeRecordProcessing();
        }
    }

    public static Writer createWriter(Context context, String fileName) throws IOException, InterruptedException {
        return new OutputStreamWriter(TaskOutputStreamFactory.createOutputStream(context, fileName));
    }

}
