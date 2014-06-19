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

import com.bc.calvalus.processing.ma.Header;
import com.bc.calvalus.processing.ma.MAConfig;
import com.bc.calvalus.processing.ma.MAMapper;
import com.bc.calvalus.processing.ma.Record;
import com.bc.calvalus.processing.ma.RecordSource;
import com.bc.calvalus.processing.ma.RecordWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import java.io.IOException;
import java.util.Iterator;

/**
 * A input format for match-up comparison.
 * Distinguishes between the reference data and the result of the different extractions.
 *
 * @author MarcoZ
 */
public class MACompareInputFormat extends FileInputFormat<Text, RecordWritable> {

    @Override
    public RecordReader<Text, RecordWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        if (fileSplit.getPath().getName().equals("part-r-00000")) {
            // output from MAReducer
            return new SequenceFileRecordReader<Text, RecordWritable>();
        } else {
            // reference data
            return new RecordSourceRecordReader();
        }
    }

    @Override
    protected boolean isSplitable(JobContext context, Path path) {
        return false;
    }

    private static class RecordSourceRecordReader extends RecordReader<Text, RecordWritable> {

        private Header header;
        private Iterator<Record> records;
        private boolean hasHeaderBeenRead;
        private Text currentKey;
        private RecordWritable currentValue;


        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            MAConfig maConfig = MAConfig.get(context.getConfiguration());
            try {
                RecordSource referenceRecordSource = maConfig.createRecordSource();
                header = referenceRecordSource.getHeader();
                records = referenceRecordSource.getRecords().iterator();
                currentKey = new Text();
                hasHeaderBeenRead = false;
            } catch (Exception e) {
                throw new IOException("Failed to open record source.", e);
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!hasHeaderBeenRead) {
                hasHeaderBeenRead = true;
                currentKey.set(MAMapper.HEADER_KEY);
                currentValue = new RecordWritable(header.getAttributeNames(), header.getAnnotationNames());
                return true;
            } else {
                if (records.hasNext()) {
                    Record selectedRecord = records.next();
                    currentKey.set(String.format("%06d_%s", selectedRecord.getId(), "#"));
                    currentValue = new RecordWritable(selectedRecord.getAttributeValues(), selectedRecord.getAnnotationValues());
                    return true;
                }
            }
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return currentKey;
        }

        @Override
        public RecordWritable getCurrentValue() throws IOException, InterruptedException {
            return currentValue;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {

        }
    }
}
