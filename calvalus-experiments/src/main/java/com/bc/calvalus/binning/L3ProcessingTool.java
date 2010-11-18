package com.bc.calvalus.binning;

import com.bc.calvalus.experiments.processing.L2ProcessingMapper;
import com.bc.calvalus.experiments.processing.N1InputFormat;
import com.bc.calvalus.experiments.processing.N1InterleavedInputFormat;
import com.bc.calvalus.experiments.util.Args;
import com.bc.calvalus.experiments.util.CalvalusLogger;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.text.MessageFormat;
import java.util.logging.Logger;

/**
 * Creates and runs Hadoop job for L3 processing. Expects input directory
 * with MERIS L1 product(s) and output directory path to be created and filled
 * with outputs.
 * <p/>
 * Call with:
 * <pre>
 *    hadoop jar target/calvalus-experiments-0.1-SNAPSHOT-job.jar \
 *    com.bc.calvalus.experiments.processing.L2ProcessingTool \
 *    hdfs://cvmaster00:9000/input \
 *    hdfs://cvmaster00:9000/output \
 *    (n1|n3|sliced|lineinterleaved) \
 *    [ndvi|radiometry|c2r] [-splits=n] [-tileHeight=h]
 * </pre>
 */
public class L3ProcessingTool extends Configured implements Tool {

    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public int run(String[] args) throws Exception {

        try {
            // parse command line arguments
            Args options = new Args(args);
            String destination = options.getArgs()[0];
            LOG.info(MessageFormat.format("start L3 processing to {0}", destination));
            long startTime = System.nanoTime();

            // construct job and set parameters and handlers
            Job job = new Job(getConf(), "L3");
            job.setJarByClass(getClass());

            job.getConfiguration().setInt("io.file.buffer.size", 1024*1024); // default is 4096

            job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");
            job.getConfiguration().set("mapred.reduce.tasks.speculative.execution", "false");
            // disable reuse for now....
            // job.getConfiguration().set("mapred.job.reuse.jvm.num.tasks", "-1");
            job.getConfiguration().set("mapred.child.java.opts", "-Xmx1024m");
            job.setNumReduceTasks(10); // todo - make configurable

            if (options.get(L2ProcessingMapper.TILE_HEIGHT_OPTION) != null) {
                job.getConfiguration().set(L2ProcessingMapper.TILE_HEIGHT_OPTION, options.get(L2ProcessingMapper.TILE_HEIGHT_OPTION));
            }

            job.setInputFormatClass(N1InputFormat.class);
            job.getConfiguration().setInt(N1InputFormat.NUMBER_OF_SPLITS, 1);

            job.setMapperClass(L3ProcessingMapper.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(SpatialBin.class);

            job.setPartitionerClass(L3Partitioner.class);

            job.setReducerClass(L3ProcessingReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(TemporalBin.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // provide input and output directories to job
            FileInputFormat.addInputPath(job, new Path("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2008/06/01"));
            FileInputFormat.addInputPath(job, new Path("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2008/06/02"));
            FileInputFormat.addInputPath(job, new Path("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2008/06/03"));
            FileInputFormat.addInputPath(job, new Path("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2008/06/04"));
            FileInputFormat.addInputPath(job, new Path("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2008/06/05"));
            FileInputFormat.addInputPath(job, new Path("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2008/06/06"));
            FileInputFormat.addInputPath(job, new Path("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2008/06/07"));
            Path output = new Path(destination);
            FileOutputFormat.setOutputPath(job, output);

            int result = job.waitForCompletion(true) ? 0 : 1;
            long stopTime = System.nanoTime();
            LOG.info(MessageFormat.format("stop L3 processing after {0} sec", (stopTime - startTime) / 1E9));
            return result;

        } catch (Exception ex) {

            System.err.println("failed: " + ex.getMessage());
            return 1;

        }

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new L3ProcessingTool(), args));
    }
}
