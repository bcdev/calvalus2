package com.bc.calvalus.experiments.add;

import com.bc.calvalus.hadoop.io.ByteArrayWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.List;

public class RasterInputFormat extends FileInputFormat {
    public static final String RASTER_WIDTH_PROPERTY = "raster.width";
    public static final String RASTER_HEIGHT_PROPERTY = "raster.height";

    public RasterInputFormat() {
        // Say something just to let me know that we arrived here
        System.out.println("###### RasterInputFormat: " + this);
    }

    @Override
    protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
        return super.computeSplitSize(blockSize, minSize, maxSize);
    }

    /**
     * Generate the list of files and make them into FileSplits.
     */
    @Override
    public List<FileSplit> getSplits(JobContext job) throws IOException {
        List<FileSplit> splits = (List<FileSplit>) super.getSplits(job);
        // Say something just to let me know that we arrived here
        for (int i = 0; i < splits.size(); i++) {
            FileSplit fileSplit = splits.get(i);
            System.out.println(this + ": split[" + i + "]" + fileSplit);
        }
        return splits;
    }

    /**
     * Create a record reader for a given split. The framework will call
     * {@link org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit , org.apache.hadoop.mapreduce.TaskAttemptContext)} before
     * the split is used.
     *
     * @param split   the split to be read
     * @param context the information about the task
     * @return a new record reader
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<IntWritable, ByteArrayWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Path file = ((FileSplit) split).getPath();
        // Say something just to let me know that we arrived here
        System.out.println("Creating raster reader for split " + split);
        FSDataInputStream stream = file.getFileSystem(context.getConfiguration()).open(file);
        return new RasterRecordReader(stream,
                                      context.getConfiguration().getInt(RASTER_WIDTH_PROPERTY, -1),
                                      context.getConfiguration().getInt(RASTER_HEIGHT_PROPERTY, -1));
    }
}
