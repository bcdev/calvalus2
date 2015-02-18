/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.ma.compare;

import com.bc.calvalus.processing.ma.CsvRecordWriter;
import com.bc.calvalus.processing.ma.Header;
import com.bc.calvalus.processing.ma.MAConfig;
import com.bc.calvalus.processing.ma.Record;
import com.bc.calvalus.processing.ma.RecordSource;
import com.bc.calvalus.processing.ma.TaskOutputStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;

public class MACompareReducer extends Reducer<MAKey, IndexedRecordWritable, NullWritable, NullWritable> {

    private RecordMerger recordMerger;
    private Header referenceHeader;
    private Iterator<Record> referenceRecords;
    private int currentReferenceId;
    private Record currentReferenceRecord;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();

        MAConfig maConfig = MAConfig.get(context.getConfiguration());
        try {
            RecordSource referenceRecordSource = maConfig.createRecordSource();
            referenceHeader = referenceRecordSource.getHeader();
            referenceRecords = referenceRecordSource.getRecords().iterator();
            currentReferenceId = -999;
            currentReferenceRecord = null;
        } catch (Exception e) {
            throw new IOException(e);
        }

        String[] identifier = conf.getStrings("calvalus.ma.identifiers");

        CsvRecordWriter csvRecordWriterALL = getCsvRecordWriter(context, "ALL");
        CsvRecordWriter csvRecordWriterCBQ = getCsvRecordWriter(context, "CBQ");
        CsvRecordWriter[] csvRecordWriterIBQ = new CsvRecordWriter[identifier.length];
        for (int i = 0; i < csvRecordWriterIBQ.length; i++) {
            csvRecordWriterIBQ[i] = getCsvRecordWriter(context, "IBQ_" + identifier[i]);
        }

        recordMerger = new RecordMerger(identifier,
                                        csvRecordWriterALL,
                                        csvRecordWriterCBQ,
                                        csvRecordWriterIBQ);
    }

    private CsvRecordWriter getCsvRecordWriter(Context context, String identifier) throws IOException, InterruptedException {
        return new CsvRecordWriter(createWriter(context, identifier + "-records-all.txt"),
                                   createWriter(context, identifier + "-records-agg.txt"));
    }

    @Override
    protected void reduce(MAKey key, Iterable<IndexedRecordWritable> records, Context context) throws IOException, InterruptedException {
        int keyReferenceId = key.getReferenceId();
        if (keyReferenceId == MAKey.HEADER_KEY) {
            recordMerger.processHeader(referenceHeader.getAttributeNames(), records);
        } else {
            while (currentReferenceId < keyReferenceId && referenceRecords.hasNext()) {
                currentReferenceRecord = referenceRecords.next();
                currentReferenceId = currentReferenceRecord.getId();
            }
            String textKey = String.format("%06d_%s", keyReferenceId, key.getProductName());
            recordMerger.processData(textKey, currentReferenceRecord.getAttributeValues(), records);
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        recordMerger.finalizeRecordProcessing();
    }

    private static Writer createWriter(Context context, String fileName) throws IOException, InterruptedException {
        return new OutputStreamWriter(TaskOutputStreamFactory.createOutputStream(context, fileName));
    }

}
