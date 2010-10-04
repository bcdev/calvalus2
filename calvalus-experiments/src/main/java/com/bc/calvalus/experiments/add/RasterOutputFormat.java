package com.bc.calvalus.experiments.add;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RasterOutputFormat extends FileOutputFormat {
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Path file = getDefaultWorkFile(job, "");
        FileSystem fs = file.getFileSystem(job.getConfiguration());
        FSDataOutputStream stream = fs.create(file);
        return new RasterRecordWriter(stream);
    }
}