package com.bc.calvalus.experiments.processing;

import com.bc.calvalus.commons.Args;
import com.bc.calvalus.commons.CalvalusLogger;
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
 * <p/>
 * Call with:
 * <pre>
 *    hadoop jar target/calvalus-experiments-0.2-SNAPSHOT-job.jar \
 *    com.bc.calvalus.experiments.processing.L2ProcessingTool \
 *    hdfs://cvmaster00:9000/input \
 *    hdfs://cvmaster00:9000/output \
 *    (n1|n3|sliced|lineinterleaved) \
 *    [ndvi|radiometry|c2r] [-splits=n] [-tileHeight=h]
 * </pre>
 */
public class L2ProcessingTool extends Configured implements Tool {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String SPLITS_OPTION = "splits";
    private static final String SPLIT_SIZE_OPTION = "splitSize";

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

            // create job name
            StringBuilder jobName = new StringBuilder("L2: ");
            jobName.append(operator);
            jobName.append(" of ");
            jobName.append(format);
            jobName.append(" ");
            int lastSlash = source.lastIndexOf("/");
            jobName.append(source.substring(lastSlash+1));
            if (options.get(L2ProcessingMapper.TILE_HEIGHT_OPTION) != null) {
                jobName.append(" ");
                jobName.append(L2ProcessingMapper.TILE_HEIGHT_OPTION);
                jobName.append("=");
                jobName.append(options.get(L2ProcessingMapper.TILE_HEIGHT_OPTION));
            }
            if (options.get(SPLITS_OPTION) != null) {
                jobName.append(" ");
                jobName.append(SPLITS_OPTION);
                jobName.append("=");
                jobName.append(options.get(SPLITS_OPTION));
            }
            if (options.get(SPLIT_SIZE_OPTION) != null) {
                jobName.append(" ");
                jobName.append(SPLIT_SIZE_OPTION);
                jobName.append("=");
                jobName.append(options.get(SPLIT_SIZE_OPTION));
            }
            LOG.info("jobname: "+ jobName.toString());
            // construct job and set parameters and handlers
            Job job = new Job(getConf(), jobName.toString());
            job.setJarByClass(getClass());

            job.getConfiguration().setInt("io.file.buffer.size", 1024*1024); // default is 4096

            job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");
            job.getConfiguration().set("mapred.reduce.tasks.speculative.execution", "false");
// disable reuse for now....
//            job.getConfiguration().set("mapred.job.reuse.jvm.num.tasks", "-1");
            job.getConfiguration().set("mapred.child.java.opts", "-Xmx1024m");
            job.getConfiguration().set("mapred.reduce.tasks", "0");
            job.getConfiguration().set(L2ProcessingMapper.OPERATOR_OPTION, operator);
            if (options.get(L2ProcessingMapper.TILE_HEIGHT_OPTION) != null) {
                job.getConfiguration().set(L2ProcessingMapper.TILE_HEIGHT_OPTION, options.get(L2ProcessingMapper.TILE_HEIGHT_OPTION));
            }

            // register handlers for splitting, map, and output
            // distinguish formats
            if ("n1".equals(format)) {

                int numberOfSplits = (int) options.getLong(SPLITS_OPTION, 1);
                job.setInputFormatClass(N1InputFormat.class);
                job.getConfiguration().setInt(N1InputFormat.NUMBER_OF_SPLITS, numberOfSplits);

            } else if ("n3".equals(format)) {

                int numberOfSplits = (int) options.getLong(SPLITS_OPTION, 3);
                job.setInputFormatClass(N1InputFormat.class);
                job.getConfiguration().setInt(N1InputFormat.NUMBER_OF_SPLITS, numberOfSplits);

            } else if ("lineinterleaved".equals(format)) {

                job.setInputFormatClass(N1InterleavedInputFormat.class);
                long splitSize = options.getLong(SPLIT_SIZE_OPTION, -1);
                if (splitSize != -1) {
                    job.getConfiguration().setLong(N1InterleavedInputFormat.SPLIT_SIZE_PARAM, splitSize);
                }

            } else if ("sliced".equals(format)) {

                int numberOfSplits = (int) options.getLong(SPLITS_OPTION, 1);
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

            System.err.println("usage:   L2ProcessingTool <source> <destination> <format> [<operator>]");
            System.err.println("example: L2ProcessingTool hdfs://cvmaster00:9000/data/experiments/n1/MERIS-RR-2010-08 hdfs://cvmaster00:9000/output/n1/MERIS-RR-2010-08 n1 ndvi");
            System.err.println("formats: n1, n3, sliced, lineinterleaved");
            System.err.println("operators: ndvi, radiometry");
            return 1;
        }

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new L2ProcessingTool(), args));
    }
}
