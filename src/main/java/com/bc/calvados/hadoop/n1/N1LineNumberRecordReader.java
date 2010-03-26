package com.bc.calvados.hadoop.n1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


public class N1LineNumberRecordReader extends RecordReader<IntWritable, IntWritable> {

    private static final int RECORD_HEIGHT = 16;
    private int yStart;
    private int height;
    private int nextLine;
    private IntWritable key = new IntWritable();
    private IntWritable value = new IntWritable();


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
        N1LineInputSplit split = (N1LineInputSplit) inputSplit;
        yStart = split.getYStart();
        height = split.getHeight();
        nextLine = yStart;
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
        if (nextLine < yStart + height) {
            key.set(nextLine);
            int actualHeight = 0;
            while ((nextLine + actualHeight <= yStart + height) && (actualHeight < RECORD_HEIGHT)) {
                actualHeight++;
            }
            nextLine += actualHeight;
            value.set(actualHeight);
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
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
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
    public IntWritable getCurrentValue() throws IOException, InterruptedException {
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
        if (nextLine == yStart) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (nextLine - yStart) / (float)(height));
        }
    }

    /**
     * Close the record reader.
     */
    @Override
    public void close() throws IOException {
       // nothing
    }
}