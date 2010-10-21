package com.bc.calvalus.experiments.processing;

import com.bc.calvalus.experiments.util.Args;
import com.bc.calvalus.experiments.util.CalvalusLogger;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
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
 * <p/>
 * Call with:
 * <pre>
 *    hadoop jar target/calvalus-experiments-0.1-SNAPSHOT-job.jar \
 *    com.bc.calvalus.experiments.processing.ProcessingTool \
 *    hdfs://cvmaster00:9000/input \
 *    hdfs://cvmaster00:9000/output \
 *    (n1|n3|sliced|lineinterleaved) \
 *    [ndvi|radiometry] [-splits=n] [-tileHeight=h]
 * </pre>
 */
public class ProcessingTool extends Configured implements Tool {

    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public int run(String[] args) throws Exception {

        try {
            // parse command line arguments
            Args options = new Args(args);
            String source = options.getArgs()[0];
            String destination = options.getArgs()[1];
            String format = options.getArgs()[2];
            String operator = (options.getArgs().length > 3) ? options.getArgs()[3] : "ndvi";
            LOG.info("start processing " + operator + " of " + format + " " + source + " to " + destination);
            long startTime = System.nanoTime();

            // construct job and set parameters and handlers
            Job job = new Job(getConf(), "L2HadoopTest");
            job.setJarByClass(getClass());
            job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");
            job.getConfiguration().set("mapred.reduce.tasks.speculative.execution", "false");
            job.getConfiguration().set("mapred.job.reuse.jvm.num.tasks", "-1");
            job.getConfiguration().set("mapred.child.java.opts", "-Xmx1024m");
            job.getConfiguration().set("mapred.reduce.tasks", "0");
            job.getConfiguration().set(L2ProcessingMapper.OPERATOR_OPTION, operator);
            if (options.get(L2ProcessingMapper.TILE_HEIGHT_OPTION) != null) {
                job.getConfiguration().set(L2ProcessingMapper.TILE_HEIGHT_OPTION, options.get(L2ProcessingMapper.TILE_HEIGHT_OPTION));
            }

            // register handlers for splitting, map, and output
            // distinguish formats
            if ("n1".equals(format)) {

                int numberOfSplits = (int) options.getLong("splits", 1);
                job.setInputFormatClass(N1InputFormat.class);
                job.getConfiguration().setInt(N1InputFormat.NUMBER_OF_SPLITS, numberOfSplits);

            } else if ("n3".equals(format)) {

                int numberOfSplits = (int) options.getLong("splits", 3);
                job.setInputFormatClass(N1InputFormat.class);
                job.getConfiguration().setInt(N1InputFormat.NUMBER_OF_SPLITS, numberOfSplits);

            } else if ("lineinterleaved".equals(format)) {

                job.setInputFormatClass(N1InterleavedInputFormat.class);
                long splitSize = options.getLong("splitSize", -1);
                if (splitSize != -1) {
                    job.getConfiguration().setLong(N1InterleavedInputFormat.SPLIT_SIZE_PARAM, splitSize);
                }

            } else if ("sliced".equals(format)) {

                int numberOfSplits = (int) options.getLong("splits", 1);
                job.setInputFormatClass(N1InputFormat.class);
                job.getConfiguration().setInt(N1InputFormat.NUMBER_OF_SPLITS, numberOfSplits);

            } else {
                throw new IllegalArgumentException("one of lineinterleaved, sliced, n1, n3 expected for format, found " + format);
            }

            job.setMapperClass(L2ProcessingMapper.class);
            // XXX use default reducer
//            job.setReducerClass(XXXReducer.class);

            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // provide input and output directories to job
            Path input = new Path(source);
            Path output = new Path(destination);
            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);

            int result = job.waitForCompletion(true) ? 0 : 1;
            long stopTime = System.nanoTime();
            LOG.info("stop  processing " + operator + " of " + format + " " + source + " after " + ((stopTime - startTime) / 1E9) +  " sec");
            return result;

        } catch (IllegalArgumentException ex) {

            System.err.println("failed: " + ex.getMessage());
            return 1;

        } catch (ArrayIndexOutOfBoundsException ex) {

            System.err.println("usage:   ProcessingTool <source> <destination> <format> [<operator>]");
            System.err.println("example: ProcessingTool hdfs://cvmaster00:9000/data/experiments/n1/MERIS-RR-2010-08 hdfs://cvmaster00:9000/output/n1/MERIS-RR-2010-08 n1 ndvi");
            System.err.println("formats: n1, n3, sliced, lineinterleaved");
            System.err.println("operators: ndvi, radiometry");
            return 1;
        }

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ProcessingTool(), args));
    }
}
