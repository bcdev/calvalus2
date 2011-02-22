package com.bc.calvalus.processing.beam;

import com.bc.calvalus.binning.SpatialBin;
import com.bc.calvalus.binning.TemporalBin;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.shellexec.ExecutablesInputFormat;
import com.bc.calvalus.processing.shellexec.FileUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.logging.Logger;

/**
 * Creates and runs Hadoop job for processing with an executable.
 * Takes xml request file as parameter. The request file specifies processor,
 * package, version, output directory, inputs and processing parameters.
 * The executable may already be installed on the cluster
 * or provided as Calvalus executable processor package.
 * Output dir is interpreted by the client that cleans up the directory.
 * All other parameters are forwarded to the split function (uses inputs) and
 * to the mappers (use all other parameters). After processing is complete the output
 * directory should be filled with output files, one per input.
 * <p/>
 * Call with:
 * <pre>
 *    hadoop jar target/calvalus-experiments-0.1-SNAPSHOT-job.jar \
 *    com.bc.calvalus.processing.beam.BeamOperatorTool \
 *    request.xml \
 *    [-wait=false]
 * </pre>
 *
 * @author Boe
* @author Norman Fomferra
 * @author Marco Zuehlke
 */
public class BeamOperatorTool extends Configured implements Tool {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static Options options = new Options();

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new BeamOperatorTool(), args));
    }

    /**
     * Constructs job for mapper-only processing with executable and submits it.
     * Waits until job completes.
     * @param args  command line parameters, one with request file path expected
     * @return  whether processing is successful (0) or failed (1)
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        // parse command line arguments
        CommandLineParser commandLineParser = new PosixParser();
        final CommandLine commandLine = commandLineParser.parse(options, args);
        String[] remainingArgs = commandLine.getArgs();
        if (remainingArgs.length == 0) {
            throw new IllegalStateException("No request file specified.");
        }
        String requestPath = remainingArgs[0];
        try {
            // parse request
            final String requestContent = FileUtil.readFile(requestPath);  // we need the content later on
            WpsConfig wpsConfig = new WpsConfig(requestContent);
            String requestOutputDir = wpsConfig.getRequestOutputDir();
            String identifier = wpsConfig.getIdentifier();

            // construct job and set parameters and handlers
            Job job = new Job(getConf(), identifier);
            Configuration conf = job.getConfiguration();
            conf.set("calvalus.request", requestContent);

            // look up job jar either by class (if deployed) or by path (idea)
            // job.setJarByClass(getClass());
            String pathname = "lib/calvalus-processing-0.1-SNAPSHOT-job.jar";
            if (!new File(pathname).exists()) {
                pathname = "calvalus-processing/target/calvalus-processing-0.1-SNAPSHOT-job.jar";
                if (!new File(pathname).exists()) {
                    throw new IllegalArgumentException("Cannot find job jar");
                }
            }
            conf.set("mapred.jar", pathname);


            // clear output directory
            final Path outputPath = new Path(requestOutputDir);
            final FileSystem fileSystem = outputPath.getFileSystem(getConf());
            fileSystem.delete(outputPath, true);
             FileOutputFormat.setOutputPath(job, outputPath);

            job.setInputFormatClass(ExecutablesInputFormat.class);

            if (wpsConfig.isLevel3()) {
                job.setNumReduceTasks(16);

                job.setMapperClass(L3Mapper.class);
                job.setMapOutputKeyClass(LongWritable.class);
                job.setMapOutputValueClass(SpatialBin.class);

                job.setPartitionerClass(L3Partitioner.class);

                job.setReducerClass(L3Reducer.class);
                job.setOutputKeyClass(LongWritable.class);
                job.setOutputValueClass(TemporalBin.class);

                job.setOutputFormatClass(SequenceFileOutputFormat.class);
                // todo - scan all input paths, collect all products and compute min start/ max stop sensing time
            } else {
                job.setMapperClass(BeamOperatorMapper.class);
                job.setNumReduceTasks(0);
                //job.setOutputFormatClass(TextOutputFormat.class);
                //job.setOutputKeyClass(Text.class);
                //job.setOutputValueClass(Text.class);
            }
            conf.set("hadoop.job.ugi", "hadoop,hadoop");  // user hadoop owns the outputs
            conf.set("mapred.map.tasks.speculative.execution", "false");
            conf.set("mapred.reduce.tasks.speculative.execution", "false");
            //conf.set("mapred.child.java.opts", "-Xmx1024m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8009");
            conf.set("mapred.child.java.opts", "-Xmx1024m");

            BeamCalvalusClasspath.configure(wpsConfig.getProcessorPackage(), conf);

            LOG.info("start processing " + identifier + " (" + requestPath + ")");
            long startTime = System.nanoTime();
            boolean success = job.waitForCompletion(true);
            int result = success ? 0 : 1;
            long stopTime = System.nanoTime();
            LOG.info("stop  processing "  + identifier + " (" + requestPath + ")" + " after " + ((stopTime - startTime) / 1E9) +  " sec");

            if (success && wpsConfig.isLevel3()) {
                FileSystem outputPathFileSystem = outputPath.getFileSystem(conf);
                FSDataOutputStream os = outputPathFileSystem.create(new Path(outputPath, BeamL3Config.L3_REQUEST_FILENAME));
                os.writeBytes(requestContent);

                final String formatterOutput = conf.get("calvalus.l3.formatter.output");
                if (formatterOutput != null) {
    //                LOG.info(MessageFormat.format("formatting to {0}", formatterOutput));
    //                L3Formatter formatter = new L3Formatter();
    //                formatter.setConf(conf);
    //                result = formatter.run(args);
                } else {
                    LOG.info(MessageFormat.format("no formatting performed", formatterOutput));
                }
            }
            return result;

        } catch (IllegalArgumentException e) {

            System.err.println("failed: " + e.getMessage());
            return 1;

        } catch (ArrayIndexOutOfBoundsException e) {

            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("beamexec <request-file>",
                                    "submit a processing request for a BEAM operator processor",
                                    options,
                                    "Example: beamexec beam-l2-sample-request.xml");
            return 1;

        } catch (FileNotFoundException e) {

            System.err.println("request file " + requestPath + " not found: " + e.getMessage());
            return 1;

        } catch (IOException e) {

            System.err.println("failed handling request file " + requestPath + ": " + e.getMessage());
            return 1;

        }
    }
}
