package com.bc.calvalus.experiments.add;

import com.bc.calvalus.processing.hadoop.ByteArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInputStream;
import java.io.IOException;
import java.text.MessageFormat;

public class RasterRecordReader extends RecordReader<IntWritable, ByteArrayWritable> {
    private final DataInputStream stream;
    private final int width;
    private final int height;
    private int lineIndex;
    private IntWritable currentKey;
    private ByteArrayWritable currentValue;

    public RasterRecordReader(DataInputStream stream, int width, int height) {
        this.stream = stream;
        this.width = width;
        this.height = height;
    }

    /**
     * Get the current key
     *
     * @return the current key or null if there is no current key
     */
    @Override
    public IntWritable getCurrentKey() {
        return currentKey;
    }

    /**
     * Get the current value.
     *
     * @return the object that was read
     */
    @Override
    public ByteArrayWritable getCurrentValue() {
        return currentValue;
    }

    /**
     * Called once at initialization.
     *
     * @param split   the split that defines the range of records to read
     * @param context the information about the task
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
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
        if (currentKey == null) {
            currentKey = new IntWritable();
            currentValue = new ByteArrayWritable(width);
        }

        byte[] data = currentValue.getArray();
        int count = stream.read(data);
        if (count == -1) {
            return false;
        } else if (count < data.length) {
            throw new IOException(MessageFormat.format("Incomplete record: expected {0}, got {1} elements", data.length, count));
        }
        currentKey.set(lineIndex++);
        return true;
    }

    /**
     * Close this {@link org.apache.hadoop.mapred.InputSplit} to future operations.
     *
     * @throws java.io.IOException
     */
    @Override
    public void close() throws IOException {
        stream.close();
    }

    /**
     * How much of the input has the {@link org.apache.hadoop.mapred.RecordReader} consumed i.e.
     * has been processed by?
     *
     * @return progress from <code>0.0</code> to <code>1.0</code>.
     * @throws java.io.IOException
     */
    @Override
    public float getProgress() throws IOException {
        return (float) lineIndex / (float) height;
    }
}
