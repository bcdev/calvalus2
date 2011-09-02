package com.bc.calvalus.processing.converter;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.shellexec.ExecutablesInputFormat;
import com.bc.calvalus.processing.shellexec.FileUtil;
import com.bc.calvalus.processing.shellexec.XmlDoc;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Logger;

import static com.bc.calvalus.processing.hadoop.HadoopProcessingService.DEFAULT_BEAM_BUNDLE;
import static com.bc.calvalus.processing.hadoop.HadoopProcessingService.addBundleToClassPath;

/**
 * Creates and runs Hadoop job for converting with a converter.
 * Takes xml request file as parameter. The request file specifies converter,
 * output directory, inputs, and optionally conversion parameters.
 * All parameters are forwarded to the split function (uses inputs) and
 * to the mappers (use all other parameters). After conversion is complete the output
 * directory should be filled with output files, one per input.
 * <p/>
 * Call with:
 * <pre>
 *    hadoop jar target/calvalus-experiments-0.3-SNAPSHOT-job.jar \
 *    com.bc.calvalus.processing.shellexec.ExecutablesTool \
 *    l2gen-request.xml \
 *    [-wait=false]
 * </pre>
 *
 * @author Boe
 */
public class ConverterTool extends Configured implements Tool {

    private static final Logger LOG = CalvalusLogger.getLogger();

    //private static final String TYPE_XPATH = "/wps:Execute/ows:Identifier";
    //private static final String OUTPUT_DIR_XPATH = "/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.output.dir']/wps:Data/wps:LiteralData";
    private static final String TYPE_XPATH = "/Execute/Identifier";
    private static final String OUTPUT_DIR_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.output.dir']/Data/Reference/@href";

    private static Options options;
    private static final String PRIORITY_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.priority']/Data/LiteralData";

    static {
        options = new Options();
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ConverterTool(), args));
    }

    /**
     * Constructs job for mapper-only conversion from seq to dim or part to nc and submits it.
     * Waits until job completes.
     * @param args  command line parameters, one with request file path expected
     * @return  whether processing was successful (0) or failed (1)
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {

        String requestPath = null;
        try {
            // parse command line arguments
            CommandLineParser commandLineParser = new PosixParser();
            final CommandLine commandLine = commandLineParser.parse(options, args);
            requestPath = commandLine.getArgs()[0];

            // parse request
            final String requestContent = FileUtil.readFile(requestPath);
            final XmlDoc request = new XmlDoc(requestContent);
            final String requestType = request.getString(TYPE_XPATH);
            final String requestPriority = request.getString(PRIORITY_XPATH, "LOW");  // one of VERY_LOW, LOW, NORMAL, HIGH, VERY_HIGH

            LOG.info("start processing " + requestType + " (" + requestPath + ")");
            long startTime = System.nanoTime();

            // construct job and set parameters and handlers
            Job job = new Job(getConf(), requestPath);
            job.getConfiguration().set("calvalus.request", requestContent);
            job.getConfiguration().set("mapred.job.priority", requestPriority);

            job.setInputFormatClass(ExecutablesInputFormat.class);
            job.setMapperClass(ConverterMapper.class);
            job.getConfiguration().set("hadoop.job.ugi", "hadoop,hadoop");  // user hadoop owns the outputs
            job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");
            job.getConfiguration().set("mapred.reduce.tasks.speculative.execution", "false");
            job.getConfiguration().set("mapred.child.java.opts", "-Xmx1024m");
            job.getConfiguration().set("mapred.reduce.tasks", "0");
            job.getConfiguration().setInt("mapred.max.map.failures.percent", 20);
            job.setOutputFormatClass(NullOutputFormat.class);

            // look up job jar either by class (if deployed) or by path (idea)
            //job.setJarByClass(getClass());
            String pathname = "lib/calvalus-processing-0.3-SNAPSHOT-job.jar";
            if (!new File(pathname).exists()) {
                pathname = "calvalus-processing/target/calvalus-processing-0.3-SNAPSHOT-job.jar";
                if (!new File(pathname).exists()) {
                    job.setJarByClass(getClass());
                    //throw new IllegalArgumentException("Cannot find job jar");
                }
            }
            job.getConfiguration().set("mapred.jar", pathname);

            addBundleToClassPath(job.getConfiguration().get(JobConfigNames.CALVALUS_BEAM_BUNDLE, DEFAULT_BEAM_BUNDLE), job.getConfiguration());
            //addBundleToClassPath(job.getConfiguration().get(JobConfNames.CALVALUS_L2_BUNDLE), job.getConfiguration());

            int result = job.waitForCompletion(true) ? 0 : 1;

            long stopTime = System.nanoTime();
            LOG.info("stop processing "  + requestType + " (" + requestPath + ")" + " after " + ((stopTime - startTime) / 1E9) +  " sec");
            return result;

        } catch (IllegalArgumentException e) {

            System.err.println("failed: " + e.getMessage());
            return 1;

        } catch (ArrayIndexOutOfBoundsException e) {

            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("convert.sh <request-file>",
                                    "submit a conversion request",
                                    options,
                                    "Example: convert.sh convert-l2-sample-request.xml");
            return 1;

        } catch (FileNotFoundException e) {

            System.err.println("request file " + requestPath + " not found: " + e.getMessage());
            return 1;

        } catch (IOException e) {

            System.err.println("failed reading request file " + requestPath + ": " + e.getMessage());
            return 1;

        }
    }
}
