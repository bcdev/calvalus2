package com.bc.calvalus.processing.beam;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Generator of splits for
 *   case A "single split",
 *   case B 'multiple split' and.
 *   case C 'separate slices'.
 */
public class N1InputFormat extends FileInputFormat {

    public static final String NUMBER_OF_SPLITS = "splits.number";

    /**
     * Generate the list of files and make them into FileSplits.
     */
    @Override
    public List<FileSplit> getSplits(JobContext job) throws IOException {
        List<FileSplit> blocks = (List<FileSplit>) super.getSplits(job);

        final int numSplits = job.getConfiguration().getInt(NUMBER_OF_SPLITS, 1);
        if (numSplits == 1) {
            return blocks;
        }

        List<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
        for (FileSplit block : blocks) {
            for (int i = 0; i < numSplits; i++) {

                Path path = block.getPath();
                long start = block.getStart(); // should be zero, because only one block exists
                long length = block.getLength();
                String[] locations = block.getLocations();

                long splitLength = length / numSplits;
                FileSplit split = new FileSplit(path, start + splitLength * i, splitLength, locations);

                splits.add(split);
            }
        }
        return splits;
    }

    /**
     * Create a record reader for a given split. Not used here.
     */
    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }
}
