package com.bc.calvalus.experiments.add;

import com.bc.calvalus.processing.hadoop.ByteArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

public class RasterRecordWriter extends RecordWriter<IntWritable, ByteArrayWritable> {
    final DataOutputStream stream;

    public RasterRecordWriter(DataOutputStream stream) {
        this.stream = stream;
    }

    /**
     * Writes a key/value pair.
     *
     * @param key   the key to write.
     * @param value the value to write.
     * @throws java.io.IOException
     */
    @Override
    public void write(IntWritable key, ByteArrayWritable value) throws IOException {
        stream.write(value.getArray());
    }

    /**
     * Close this <code>RecordWriter</code> to future operations.
     *
     * @param context the context of the task
     * @throws java.io.IOException
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        stream.close();
    }
}