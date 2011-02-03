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
public class BeamOperatorTool extends Configured implements Tool {

    private static final Logger LOG = CalvalusLogger.getLogger();

    //private static final String TYPE_XPATH = "/wps:Execute/ows:Identifier";
    //private static final String OUTPUT_DIR_XPATH = "/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.output.dir']/wps:Data/wps:LiteralData";
    private static final String TYPE_XPATH = "/Execute/Identifier";
    private static final String OUTPUT_DIR_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.output.dir']/Data/Reference/@href";

    private static Options options;
    private static final String PROCESSOR_PACKAGE_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.processor.package']/Data/LiteralData";
    private static final String PROCESSOR_VERSION_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.processor.version']/Data/LiteralData";

    static {
        options = new Options();
    }

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

        String requestPath = null;
        try {
            // parse command line arguments
            CommandLineParser commandLineParser = new PosixParser();
            final CommandLine commandLine = commandLineParser.parse(options, args);
            requestPath = commandLine.getArgs()[0];

            // parse request
            final String requestContent = FileUtil.readFile(requestPath);  // we need the content later on
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
            job.setMapperClass(BeamOperatorMapper.class);

            //job.setJarByClass(getClass());
            job.getConfiguration().set("mapred.jar", "calvalus-processing/target/calvalus-processing-0.1-SNAPSHOT-job.jar");

            // put processor onto the classpath
            final String processorPackage = request.getString(PROCESSOR_PACKAGE_XPATH);
            final String processorVersion = request.getString(PROCESSOR_VERSION_XPATH);
            addPackageToClassPath(processorPackage + "-" + processorVersion, job.getConfiguration());

            // put BEAM and BEAM 3rd party-libs onto the classpath
            final String beamPackage = "beam";
            final String beamVersion = "4.9-SNAPSHOT";
            addPackageToClassPath(beamPackage + "-" + beamVersion, job.getConfiguration());

            job.getConfiguration().set("hadoop.job.ugi", "hadoop,hadoop");  // user hadoop owns the outputs
            job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");
            job.getConfiguration().set("mapred.reduce.tasks.speculative.execution", "false");
            //job.getConfiguration().set("mapred.child.java.opts", "-Xmx1024m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8009");
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

    private void addPackageToClassPath(String packageName, Configuration configuration) throws IOException {
        final Path beamPath = new Path("hdfs://localhost:9000/calvalus/software/0.5/" + packageName);
        final FileSystem beamFileSystem = beamPath.getFileSystem(configuration);
        final FileStatus[] beamJars = beamFileSystem.listStatus(beamPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().endsWith("jar");
            }
        });
        for (FileStatus beamJar : beamJars) {
            final Path path = beamJar.getPath();
            final Path pathWithoutProtocol = new Path(path.toUri().getPath());  // for hadoops sake!
            DistributedCache.addFileToClassPath(pathWithoutProtocol, configuration);
        }
    }
}
