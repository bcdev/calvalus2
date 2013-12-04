package com.bc.calvalus.processing.boostrapping;

import com.bc.calvalus.processing.ma.TaskOutputStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

/**
 * Writes the header key only once and all body keys to the output.
 */
public class BoostrappingReducer extends Reducer<IntWritable, Text, NullWritable, NullWritable> {

    private Writer writer;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path inputPath = new Path(conf.get(BootstrappingWorkflowItem.INPUT_FILE_PROPRTY));
        String outputFilename = "output_" + inputPath.getName();
        writer = new OutputStreamWriter(TaskOutputStreamFactory.createOutputStream(context, outputFilename));
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (key.get() == 0) {
            Text header = values.iterator().next();
            writer.write(header.toString());
        } else {
            for (Text value : values) {
                writer.write(value.toString());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        writer.close();
    }
}
