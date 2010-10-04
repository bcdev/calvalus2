package com.bc.calvalus.experiments.stx;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class StxTool extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "Stx computation");
        job.setJarByClass(StxTool.class);

        job.getConfiguration().setInt(N1LineNumberRecordReader.RECORD_HEIGHT, 1000);

        job.setInputFormatClass(N1ProductFormat.class);

        job.setMapperClass(StxMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StxWritable.class);

        job.setReducerClass(StxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StxWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);

        job.setCombinerClass(StxReducer.class);

        FileInputFormat.setInputPaths(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new StxTool(), args));
    }
}
