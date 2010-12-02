package com.bc.calvalus.b3.job;

import com.bc.calvalus.b3.IsinBinningGrid;
import com.bc.calvalus.b3.SpatialBin;
import com.bc.calvalus.b3.TemporalBin;
import com.bc.calvalus.experiments.processing.N1InputFormat;
import com.bc.calvalus.experiments.util.CalvalusLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.logging.Logger;

import static com.bc.calvalus.b3.job.L3Config.*;

/**
 * Creates and runs Hadoop job for L3 processing.
 * <pre>
 *   Usage: L3Tool <i>request-file</i>
 * </pre>
 * where <i>request-file</i> is a Java properties file.
 *
 * @author Norman Fomferra
 * @author Marco Zuehlke
 */
public class L3Tool extends Configured implements Tool {

    static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public int run(String[] args) throws Exception {

        try {
            String requestFile = args.length > 0 ? args[0] : "l3tool.properties";

            // construct job and set parameters and handlers
            Job job = new Job(getConf());
            L3Config l3Config = L3Config.loadProperties(new File(requestFile));

            Configuration conf = job.getConfiguration();
            l3Config.copyToConfiguration(conf);

            validateConfiguration(conf);

            job.setJobName(String.format("l3_ndvi_%dd_%dr",
                                         conf.getInt(CONFNAME_L3_NUM_DAYS, -1),
                                         conf.getInt(CONFNAME_L3_GRID_NUM_ROWS, -1)));

            job.setJarByClass(getClass());
            job.setNumReduceTasks(16);

            job.setInputFormatClass(N1InputFormat.class);
            conf.setInt(N1InputFormat.NUMBER_OF_SPLITS, 1);

            job.setMapperClass(L3Mapper.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(SpatialBin.class);

            job.setPartitionerClass(L3Partitioner.class);

            job.setReducerClass(L3Reducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(TemporalBin.class);

            if (conf.getBoolean("calvalus.l3.outputText", false)) {
                job.setOutputFormatClass(TextOutputFormat.class);
            } else {
                job.setOutputFormatClass(SequenceFileOutputFormat.class);
            }

            // provide input and output directories to job
            final int day1 = conf.getInt(CONFNAME_L3_START_DAY, 1);
            final int day2 = day1 - 1 + conf.getInt(CONFNAME_L3_NUM_DAYS, 4);
            for (int day = day1; day <= day2; day++) {
                String pathName = String.format(conf.get(CONFNAME_L3_INPUT), day);
                FileInputFormat.addInputPath(job, new Path(pathName));
            }
            Path output = l3Config.getOutput();
            FileOutputFormat.setOutputPath(job, output);

            // todo - scan all input paths, collect all products and compute min start/ max stop sensing time

            LOG.info(MessageFormat.format("start L3 processing to {0}", output));
            long startTime = System.nanoTime();
            int result = job.waitForCompletion(true) ? 0 : 1;
            long stopTime = System.nanoTime();
            LOG.info(MessageFormat.format("stop L3 processing after {0} sec", (stopTime - startTime) / 1E9));

            if (result != 0) {
                return result;
            }

            l3Config.writeProperties(conf, output);

            final String formatterOutput = conf.get("calvalus.l3.formatter.output");
            if (formatterOutput != null) {
                LOG.info(MessageFormat.format("formatting to {0}", formatterOutput));
                L3Formatter formatter = new L3Formatter();
                formatter.setConf(conf);
                result = formatter.run(args);
            } else {
                LOG.info(MessageFormat.format("no formatting performed", formatterOutput));
            }

            return result;

        } catch (Exception ex) {

            System.err.println("failed: " + ex.getMessage());
            ex.printStackTrace(System.err);
            return 1;

        }

    }

    private void validateConfiguration(Configuration conf) throws IOException {
        // Overwrite configuration by request parameters
        if (conf.get(CONFNAME_L3_INPUT) == null) {
            throw new IllegalArgumentException(MessageFormat.format("No input specified. {0} = null", CONFNAME_L3_INPUT));
        }
        if (conf.get(CONFNAME_L3_OUTPUT) == null) {
            throw new IllegalArgumentException(MessageFormat.format("No output specified. {0} = null", CONFNAME_L3_OUTPUT));
        }
        if (conf.get(String.format(CONFNAME_L3_AGG_i_TYPE, 0)) == null) {
            throw new IllegalArgumentException(MessageFormat.format("No aggregator specified. {0} = null", String.format(CONFNAME_L3_AGG_i_TYPE, 0)));
        }
        if (conf.get(CONFNAME_L3_NUM_SCANS_PER_SLICE) == null) {
            conf.setInt(CONFNAME_L3_NUM_SCANS_PER_SLICE, L3Config.DEFAULT_L3_NUM_SCANS_PER_SLICE);
        }
        if (conf.get(CONFNAME_L3_GRID_NUM_ROWS) == null) {
            conf.setInt(CONFNAME_L3_GRID_NUM_ROWS, IsinBinningGrid.DEFAULT_NUM_ROWS);
        }
        if (conf.get(CONFNAME_L3_NUM_DAYS) == null) {
            conf.setInt(CONFNAME_L3_NUM_DAYS, L3Config.DEFAULT_L3_NUM_NUM_DAYS);
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new L3Tool(), args));
    }
}
