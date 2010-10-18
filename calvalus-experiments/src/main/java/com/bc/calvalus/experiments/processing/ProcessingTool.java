package com.bc.calvalus.experiments.processing;

import com.bc.calvalus.experiments.util.Args;
import com.bc.calvalus.experiments.util.CalvalusLogger;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.logging.Logger;

/**
 * Creates and runs Hadoop job for L2 processing. Expects input directory
 * with MERIS L1 product(s) and output directory path to be created and filled
 * with outputs.
 * Currently accepts option -lineInterleaved to distinguish case E "interleaved"
 * from case A "single split".
 * <p/>
 * Call with:
 * <pre>
 *    hadoop jar target/calvalus-experiments-0.1-SNAPSHOT-job.jar \
 *    com.bc.calvalus.experiments.processing.ProcessingTool \
 *    hdfs://cvmaster00:9000/input \
 *    hdfs://cvmaster00:9000/output
 * </pre>
 */
public class ProcessingTool extends Configured implements Tool {

    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public int run(String[] args) throws Exception {

        System.out.println("Submitting job...");

        // parse command line arguments
        Args options = new Args(args);
        Boolean isLineInterleaved = Boolean.parseBoolean(options.get("lineInterleaved"));
        Long splitSize = options.getLong("splitSize", -1);

        // construct job and set parameters and handlers
        Job job = new Job(getConf(), "L2HadoopTest");
        job.setJarByClass(getClass());

        // register handlers for splitting, map, and output
        if (isLineInterleaved) {
            job.setInputFormatClass(N1InterleavedInputFormat.class);
            if (splitSize != -1) {
                job.getConfiguration().setLong(N1InterleavedInputFormat.SPLIT_SIZE_PARAM, splitSize);
            }
        } else {
            job.getConfiguration().setInt(SplitN1InputFormat.NUMBER_OF_SPLITS, 1);
            job.setInputFormatClass(SplitN1InputFormat.class);
        }

        job.setMapperClass(L2ProcessingMapper.class);
        // XXX use default reducer
        //job.setReducerClass(XXXReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // provide input and output directories to job
        Path input = new Path(options.getArgs()[0]);
        Path output = new Path(options.getArgs()[1]);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.out.println("CWD: " + job.getWorkingDirectory());

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ProcessingTool(), args));
    }
}
