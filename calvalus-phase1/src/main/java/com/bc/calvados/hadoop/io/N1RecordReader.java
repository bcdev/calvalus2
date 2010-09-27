package com.bc.calvados.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


public class N1RecordReader extends RecordReader<LongWritable, BytesWritable> {

    boolean firstAndOnlyRecordRead;

    private long granuleSize;
    private long start;
    private long pos;
    private long end;
    private LongWritable key = null;
    private BytesWritable value = null;
    private FSDataInputStream fileIn;


    /**
     * Called once at initialization.
     *
     * @param inputSplit   the split that defines the range of records to read
     * @param context the information about the task
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        firstAndOnlyRecordRead = false;

        N1InputSplit split = (N1InputSplit) inputSplit;
        Configuration job = context.getConfiguration();

        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(split.getPath());

        int headerSize = split.getHeaderSize();
        granuleSize = split.getGranuleSize();
        if (start == 0) {
            start = headerSize;
        } else {
            long startPos = headerSize;
            while (startPos <= start) {
                startPos += granuleSize;
            }
            start = startPos;
        }
        this.pos = start;
    }



    /**
     * Read the next key, value pair.
     *
     * @return true if a key/value pair was read
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!firstAndOnlyRecordRead && pos < end) {
            firstAndOnlyRecordRead = true;
            int recordsToRead = 0;
            while (recordsToRead * granuleSize + pos <= end) {
                recordsToRead++;
            }
            byte[] buffer = new byte[(int) (recordsToRead * granuleSize)];
            fileIn.read(buffer);
            value = new BytesWritable(buffer);
            key = new LongWritable(start);
            return true;
        }
        return false;
    }

    /**
     * Get the current key
     *
     * @return the current key or null if there is no current key
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * Get the current value.
     *
     * @return the object that was read
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * The current progress of the record reader through its data.
     *
     * @return a number between 0.0 and 1.0 that is the fraction of the data read
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return firstAndOnlyRecordRead ? 1.0f : 0.0f;
    }

    /**
     * Close the record reader.
     */
    @Override
    public void close() throws IOException {
        fileIn.close();
    }
}
