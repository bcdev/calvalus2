package com.bc.calvalus.experiments.executables;

import com.bc.calvalus.experiments.util.Args;
import com.bc.calvalus.experiments.util.CalvalusLogger;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
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
 *    com.bc.calvalus.experiments.executables.ExecutablesTool \
 *    l2gen-request.xml \
 *    [-wait=false]
 * </pre>
 *
 * @author Boe
 */
public class ExecutablesTool extends Configured implements Tool {

    private static final Logger LOG = CalvalusLogger.getLogger();

    //private static final String TYPE_XPATH = "/wps:Execute/ows:Identifier";
    //private static final String OUTPUT_DIR_XPATH = "/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.output.dir']/wps:Data/wps:LiteralData";
    private static final String TYPE_XPATH = "/Execute/Identifier";
    private static final String OUTPUT_DIR_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.output.dir']/Data/Reference/@href";

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ExecutablesTool(), args));
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

        String requestPath = null;
        try {
            // parse command line arguments
            final Args options = new Args(args);
            requestPath = options.getArgs()[0];

            // parse request
            final String requestContent = readFile(requestPath);  // we need the content later on
            final XmlDoc request = new XmlDoc(requestContent);
            final String requestType = request.getString(TYPE_XPATH);
            final String requestOutputDir = request.getString(OUTPUT_DIR_XPATH);

            // clear output directory
            final Path output = new Path(requestOutputDir);
            final FileSystem fileSystem = output.getFileSystem(getConf());
            fileSystem.delete(output, true);
            //fileSystem.mkdirs(output);

            LOG.info("start processing " + requestType + " (" + requestPath + ")");
            long startTime = System.nanoTime();

            // construct job and set parameters and handlers
            Job job = new Job(getConf(), requestPath);  // TODO improve job identification
            job.getConfiguration().set("calvalus.request", requestContent);
            job.setInputFormatClass(ExecutablesInputFormat.class);
            job.setMapperClass(ExecutablesMapper.class);
            job.setJarByClass(getClass());
            job.getConfiguration().set("hadoop.job.ugi", "hadoop,hadoop");  // user hadoop owns the outputs
            job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");
            job.getConfiguration().set("mapred.reduce.tasks.speculative.execution", "false");
            job.getConfiguration().set("mapred.child.java.opts", "-Xmx1024m");
            job.getConfiguration().set("mapred.reduce.tasks", "0");
            // TODO is this necessary?
            FileOutputFormat.setOutputPath(job, output);
            //job.setOutputFormatClass(TextOutputFormat.class);
            //job.setOutputKeyClass(Text.class);
            //job.setOutputValueClass(Text.class);

            int result = job.waitForCompletion(true) ? 0 : 1;

            long stopTime = System.nanoTime();
            LOG.info("stop  processing "  + requestType + " (" + requestPath + ")" + " after " + ((stopTime - startTime) / 1E9) +  " sec");
            return result;

        } catch (IllegalArgumentException e) {

            System.err.println("failed: " + e.getMessage());
            return 1;

        } catch (ArrayIndexOutOfBoundsException e) {

            System.err.println("usage:   ExecutablesTool <request>");
            System.err.println("example: ExecutablesTool l2gen-request.xml");
            return 1;

        } catch (FileNotFoundException e) {

            System.err.println("request file " + requestPath + " not found: " + e.getMessage());
            return 1;

        } catch (IOException e) {

            System.err.println("failed reading request file " + requestPath + ": " + e.getMessage());
            return 1;

        }
    }

    private String readFile(String path) throws IOException, FileNotFoundException {
        File file = new File(path);
        Reader in = new FileReader(file);
        char[] buffer = new char[(int) file.length()];
        in.read(buffer);
        return new String(buffer);
    }
}
