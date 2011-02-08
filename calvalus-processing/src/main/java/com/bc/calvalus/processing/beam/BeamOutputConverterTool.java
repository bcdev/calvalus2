package com.bc.calvalus.processing.beam;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.shellexec.ExecutablesInputFormat;
import com.bc.calvalus.processing.shellexec.FileUtil;
import com.bc.calvalus.processing.shellexec.XmlDoc;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;

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
 *    com.bc.calvalus.processing.shellexec.ExecutablesTool \
 *    l2gen-request.xml \
 *    [-wait=false]
 * </pre>
 *
 * @author Boe
 */
public class BeamOutputConverterTool extends Configured implements Tool {

    private static final Logger LOG = CalvalusLogger.getLogger();

    private static Options options;

    static {
        options = new Options();
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new BeamOutputConverterTool(), args));
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

        String inputPath = null;
        try {
            // parse command line arguments
            CommandLineParser commandLineParser = new PosixParser();
            final CommandLine commandLine = commandLineParser.parse(options, args);
            inputPath = commandLine.getArgs()[0];
            final String outputPath = commandLine.getArgs()[1];

            StreamingProductReader reader = new StreamingProductReader(new Path(inputPath), getConf());
            Product product = reader.readProductNodes(null, null);
            ProductIO.writeProduct(product, outputPath, ProductIO.DEFAULT_FORMAT_NAME);

            return 0;

        } catch (IllegalArgumentException e) {

            System.err.println("failed: " + e.getMessage());
            return 1;

        } catch (ArrayIndexOutOfBoundsException e) {

            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("convertseq <input-file> <output-file>",
                                    "convert a sequence file to BEAM-DIMAP format",
                                    options,
                                    "Example: convertseq hdfs://cvmaster00:9000/calvalus/outputs/meris-l2beam-99/L2_of_MER_RR__1P.N1.seq /tmp/meris-l2beam-88/L2_of_MER_RR__1P.N1.seq");
            return 1;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.err.println("file " + inputPath + " not found: " + e.getMessage());
            return 1;

        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("failed handling file " + inputPath + ": " + e.getMessage());
            return 1;

        }
    }
}
