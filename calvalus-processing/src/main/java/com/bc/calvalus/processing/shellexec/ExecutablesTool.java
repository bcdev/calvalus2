/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.shellexec;

import com.bc.calvalus.commons.CalvalusLogger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.esa.beam.util.io.FileUtils;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
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
 *    hadoop jar target/calvalus-experiments-1.5-job.jar \
 *    com.bc.calvalus.processing.shellexec.ExecutablesTool \
 *    l2gen-request.xml \
 *    [-wait=false]
 * </pre>
 *
 * @author Boe
 */
@Deprecated
public class ExecutablesTool extends Configured implements Tool {

    private static final Logger LOG = CalvalusLogger.getLogger();

    //private static final String TYPE_XPATH = "/wps:Execute/ows:Identifier";
    //private static final String OUTPUT_DIR_XPATH = "/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.output.dir']/wps:Data/wps:LiteralData";
    private static final String TYPE_XPATH = "/Execute/Identifier";
    private static final String OUTPUT_DIR_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.output.dir']/Data/Reference/@href";

    private static Options options;
    private static final String PRIORITY_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.priority']/Data/LiteralData";
    private static final String FAILURE_PERCENT_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.hadoop.mapred.max.map.failures.percent']/Data/LiteralData";
    private static final String CALVALUS_PARAMS_XPATH = "/Execute/DataInputs/Input[starts-with(Identifier,'calvalus.')]";
    private static final String PARAM_NAME_XPATH = "Identifier";
    private static final String PARAM_VALUE_XPATH = "Data/LiteralData";
    private static final String PACKAGE_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.processor.package']/Data/LiteralData";
    private static final String VERSION_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.processor.version']/Data/LiteralData";

    static final boolean PML_MODE = false;

    static {
        options = new Options();
    }

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
            CommandLineParser commandLineParser = new PosixParser();
            final CommandLine commandLine = commandLineParser.parse(options, args);
            requestPath = commandLine.getArgs()[0];

            // parse request
            final String requestContent = FileUtils.readText(new File(requestPath));  // we need the content later on
            final XmlDoc request = new XmlDoc(requestContent);
            final String requestType = request.getString(TYPE_XPATH);
            final String requestPriority = request.getString(PRIORITY_XPATH, "LOW");  // one of VERY_LOW, LOW, NORMAL, HIGH, VERY_HIGH
            //final String requestOutputDir = request.getString(OUTPUT_DIR_XPATH);
            final String failurePercent = request.getString(FAILURE_PERCENT_XPATH);

            // clear output directory
            //final Path output = new Path(requestOutputDir);
            //final FileSystem fileSystem = output.getFileSystem(getConf());
            //fileSystem.delete(output, true);
            ////fileSystem.mkdirs(output);

            LOG.info("start processing " + requestType + " (" + requestPath + ")");
            long startTime = System.nanoTime();

            // construct job and set parameters and handlers
            Job job = new Job(getConf(), requestPath);  // TODO improve job identification
            job.getConfiguration().set("calvalus.request", requestContent);
            if (requestPriority != null) {
                job.getConfiguration().set("mapred.job.priority", requestPriority);
            }

            if (PML_MODE) {
                // check package availability in software archive
                final String packageName = request.getString(PACKAGE_XPATH, (String) null);
                final String packageVersion = request.getString(VERSION_XPATH, (String) null);
                final String packageFilename = packageName + "-" + packageVersion + ".tar.gz";
                URI defaultUri = FileSystem.getDefaultUri(job.getConfiguration());
                // TODO move constants to some configuration
                final String archiveMountPath = defaultUri.toString();
                final String archiveRootPath = archiveMountPath + "/calvalus/software/0.5";

                Path archivePackage = new Path(archiveRootPath, packageFilename);
                FileSystem fs = archivePackage.getFileSystem(job.getConfiguration());
                if (!fs.exists(archivePackage)) {
                    throw new ProcessorException(archivePackage.toUri().getPath() + " installation package not found");
                }
                DistributedCache.addCacheArchive(archivePackage.toUri(), job.getConfiguration());
            }

            job.setInputFormatClass(ExecutablesInputFormat.class);
            job.setMapperClass(ExecutablesMapper.class);
            job.getConfiguration().set("hadoop.job.ugi", "hadoop,hadoop");  // user hadoop owns the outputs
            job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");
            job.getConfiguration().set("mapred.reduce.tasks.speculative.execution", "false");
            job.getConfiguration().set("mapred.child.java.opts", "-Xmx1024m");
            job.getConfiguration().set("mapred.reduce.tasks", "0");
            if (failurePercent == null) {
                job.getConfiguration().setInt("mapred.max.map.failures.percent", 20);
            }
            // TODO is this necessary?
            //FileOutputFormat.setOutputPath(job, output);
            job.setOutputFormatClass(NullOutputFormat.class);

            // look up job jar either by class (if deployed) or by path (idea)
            //job.setJarByClass(getClass());
            String pathname = "lib/calvalus-processing-1.5-job.jar";
            if (!new File(pathname).exists()) {
                pathname = "calvalus-processing/target/calvalus-processing-1.5-job.jar";
                if (!new File(pathname).exists()) {
                    throw new IllegalArgumentException("Cannot find job jar");
                }
            }
            job.getConfiguration().set("mapred.jar", pathname);

            final NodeList nodes = request.getNodes(CALVALUS_PARAMS_XPATH);
            for (int i = 0; i < nodes.getLength(); ++i) {
                Node node = nodes.item(i);
                String name = request.getString(PARAM_NAME_XPATH, node);
                String value = request.getString(PARAM_VALUE_XPATH, node);
                if (name.startsWith("calvalus.hadoop.")) {
                    String hadoopName = name.substring("calvalus.hadoop.".length());
                    job.getConfiguration().set(hadoopName, value);
                } else if (name.startsWith("calvalus.")) {
                    String calvalusName = name.substring("calvalus.".length());
                    job.getConfiguration().set(calvalusName, value);
                }
            }

            int result = job.waitForCompletion(true) ? 0 : 1;

            long stopTime = System.nanoTime();
            LOG.info("stop processing "  + requestType + " (" + requestPath + ")" + " after " + ((stopTime - startTime) / 1E9) +  " sec");
            return result;

        } catch (IllegalArgumentException e) {

            System.err.println("failed: " + e.getMessage());
            return 1;

        } catch (ArrayIndexOutOfBoundsException e) {

            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("shellexec <request-file>",
                                    "submit a processing request for an executable processor",
                                    options,
                                    "Example: shellexec l2gen-request.xml");
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
