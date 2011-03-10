package com.bc.calvalus.processing.beam;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.shellexec.FileUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
     *
     * @param args command line parameters, one with request file path expected
     * @return whether processing is successful (0) or failed (1)
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
            BeamOpProcessingType beamOpProcessingType = new BeamOpProcessingType(new JobClient(new JobConf(getConf())));
            Job job = beamOpProcessingType.createBeamHadoopJob(requestContent);

            // look up job jar either by class (if deployed) or by path (idea)
            // job.setJarByClass(getClass());
            String pathname = "lib/calvalus-processing-0.1-SNAPSHOT-job.jar";
            if (!new File(pathname).exists()) {
                pathname = "calvalus-processing/target/calvalus-processing-0.1-SNAPSHOT-job.jar";
                if (!new File(pathname).exists()) {
                    throw new IllegalArgumentException("Cannot find job jar");
                }
            }
            job.getConfiguration().set("mapred.jar", pathname);

            LOG.info("start processing " + job.getJobName() + " (" + requestPath + ")");
            long startTime = System.nanoTime();
            boolean success = job.waitForCompletion(true);
            int result = success ? 0 : 1;
            long stopTime = System.nanoTime();
            LOG.info("stop  processing " + job.getJobName() + " (" + requestPath + ")" + " after " + ((stopTime - startTime) / 1E9) + " sec");

            if (success) {
                Path outputPath = FileOutputFormat.getOutputPath(job);
                FileSystem outputPathFileSystem = outputPath.getFileSystem(job.getConfiguration());
                FSDataOutputStream os = outputPathFileSystem.create(new Path(outputPath, BeamL3Config.L3_REQUEST_FILENAME));
                os.writeBytes(requestContent);
                os.close();

                if (requestContent.contains("calvalus.formatter.parameters")) {
                    WpsConfig wpsConfig = new WpsConfig(requestContent);

                    BeamL3Config beamL3Config = BeamL3Config.create(wpsConfig.getLevel3Paramter());
                    FormatterL3Config formatterL3Config = FormatterL3Config.create(wpsConfig.getFormatterParameter());
                    String hadoopJobOutputDir = wpsConfig.getRequestOutputDir();
                    BeamL3FormattingService beamL3FormattingService = new BeamL3FormattingService(LOG, getConf());
                    result = beamL3FormattingService.format(formatterL3Config, beamL3Config, hadoopJobOutputDir);
                } else {
                    LOG.info("no formatting performed");
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
