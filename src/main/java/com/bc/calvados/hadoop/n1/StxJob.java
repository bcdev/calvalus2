package com.bc.calvados.hadoop.n1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class StxJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Job job = new Job();

        job.setInputFormatClass(N1ProductFormat.class);

        job.setMapperClass(StxMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(StxWritable.class);

        job.setReducerClass(StxReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(StxWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);

        job.setCombinerClass(StxReducer.class);

        FileInputFormat.setInputPaths(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        job.waitForCompletion(true);
    }
}
