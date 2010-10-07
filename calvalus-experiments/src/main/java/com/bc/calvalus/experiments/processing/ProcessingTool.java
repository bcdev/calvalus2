package com.bc.calvalus.experiments.processing;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


// bin/hadoop jar hadoop-tests-1.0.jar com.bc.calvalus.hadoop.eodata.AddRastersTool input output

//
public class ProcessingTool extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // construct job and set parameters and handlers
        System.out.println("Submitting job...");

        Job job = new Job(getConf(), "SimpleHadoopTest");

        job.getConfiguration().setInt(SplitN1InputFormat.NUMBER_OF_SPLITS, 1);

        job.setJarByClass(getClass());

        job.setMapperClass(L2ProcessingMapper.class);
        job.setInputFormatClass(SplitN1InputFormat.class);

        //job.setReducerClass(AddRastersReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.out.println("CWD: " + job.getWorkingDirectory());

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ProcessingTool(), args));
    }
}
