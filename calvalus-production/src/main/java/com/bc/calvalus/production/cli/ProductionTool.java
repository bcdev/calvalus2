package com.bc.calvalus.production.cli;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.*;
import com.bc.calvalus.production.hadoop.HadoopProductionServiceFactory;
import com.bc.calvalus.production.hadoop.HadoopProductionType;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.jdom.JDOMException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Calvalus production CLI tool "cpt".
 * <p/>
 * <pre>
 * usage: cpt [OPTION]... REQUEST
 *
 * The Calvalus production tool submits a production REQUEST to a Calvalus
 * production system. REQUEST must be a plain text XML file conforming to the
 * WPS Execute operation request (see
 * http://schemas.opengis.net/wps/1.0.0/wpsExecute_request.xsd). OPTION may
 * be one or more of the following:
 *  -B,--beam &lt;NAME&gt;       The name of the BEAM software bundle used for the
 *                         production. Defaults to 'beam-4.10-SNAPSHOT'.
 *  -c,--config &lt;FILE&gt;     The Calvalus configuration file (Java properties
 *                         format). Defaults to 'C:\Users\Norman\.calvalus\calvalus.config'.
 *  -C,--calvalus &lt;NAME&gt;   The name of the Calvalus software bundle used for
 *                         the production. Defaults to 'calvalus-0.3-201108'
 *     --copy &lt;FILES&gt;      Copies FILES to '/calvalus/home/&lt;user&gt;' before the
 *                         request is executed.Use the colon ':' to separate paths in SOURCES.
 *     --deploy &lt;FILES&gt;    Deploys FILES to the Calvalus bundle before the
 *                         request is executed. The Calvalus bundle given by the 'calvalus' option.
 *                         Use the colon ':' to separate multiple paths in FILES.
 *  -e,--errors            Print full Java stack trace on exceptions.
 *     --help              Prints out usage help.
 *  -q,--quite             Quite mode, only minimum console output.
 *  </pre>
 *
 * @author Marco Zuehlke
 * @author Norman Fomferra
 */
public class ProductionTool {

    private static final String TOOL_NAME = "cpt";

    private static final String DEFAULT_CONFIG_PATH = new File(ProductionServiceConfig.getUserAppDataDir(), "calvalus.config").getPath();
    private static final String DEFAULT_BEAM_BUNDLE = "beam-4.10-SNAPSHOT";
    private static final String DEFAULT_CALVALUS_BUNDLE = "calvalus-0.3-201108";

    private static final Options TOOL_OPTIONS = createCommandlineOptions();

    private boolean errors;
    private boolean quite;

    public static void main(String[] args) {
        new ProductionTool().run(args);
    }

    private void run(String[] args) {
        final CommandLine commandLine;
        try {
            commandLine = parseCommandLine(args);
        } catch (ParseException e) {
            exit("Error: " + e.getMessage() + " (use option --help for usage help)", -1);
            printHelp();
            return;
        }

        boolean hasOtherCommand = commandLine.hasOption("deploy")
                || commandLine.hasOption("kill")
                || commandLine.hasOption("copy")
                || commandLine.hasOption("help");
        List argList = commandLine.getArgList();
        if (argList.size() == 0 && !hasOtherCommand) {
            exit("Error: Missing argument REQUEST. (use option --help for usage help)", -1);
        }
        if (argList.size() > 1) {
            exit("Error: Too many arguments. (use option --help for usage help)", -1);
        }
        String requestPath = argList.size() == 1 ? (String) argList.get(0) : null;

        errors = commandLine.hasOption("errors");
        quite = commandLine.hasOption("quite");

        if (commandLine.hasOption("help")) {
            printHelp();
            return;
        }

        Map<String, String> defaultConfig = new HashMap<String, String>();
        defaultConfig.put("calvalus.hadoop.fs.default.name", "hdfs://cvmaster00:9000");
        defaultConfig.put("calvalus.hadoop.mapred.job.tracker", "cvmaster00:9001");
        defaultConfig.put("calvalus.calvalus.bundle", commandLine.getOptionValue("calvalus", DEFAULT_CALVALUS_BUNDLE));
        defaultConfig.put("calvalus.beam.bundle", commandLine.getOptionValue("beam", DEFAULT_BEAM_BUNDLE));

        ProductionService productionService = null;
        try {
            String configFile = commandLine.getOptionValue("config", DEFAULT_CONFIG_PATH);
            say(String.format("Loading Calvalus configuration '%s'...", configFile));
            Map<String, String> config = ProductionServiceConfig.loadConfig(new File(configFile), defaultConfig);
            say("Configuration loaded.");

            if (commandLine.hasOption("deploy")) {
                deployCalvalusSoftware(commandLine.getOptionValue("deploy"), config);
            }

            if (commandLine.hasOption("copy")) {
                copyFilesToHDFS(commandLine.getOptionValue("copy"), config);
            }

            if (requestPath == null && !commandLine.hasOption("kill")) {
                return;
            }

            HadoopProductionServiceFactory productionServiceFactory = new HadoopProductionServiceFactory();
            productionService = productionServiceFactory.create(config, ProductionServiceConfig.getUserAppDataDir(), new File("."));

            if (commandLine.hasOption("kill")) {
                cancelProduction(productionService, commandLine.getOptionValue("kill"), config);
            }
            if (requestPath == null) {
                return;
            }

            // todo - put WPS XML filename into ProductionRequest using parameter "calvalus.request.file" (nf)
            // todo - put WPS XML content into ProductionRequest using parameter "calvalus.request.xml" (nf)
            FileReader requestReader = new FileReader(requestPath);
            ProductionRequest request;
            try {
                say(String.format("Loading production request '%s'...", requestPath));
                request = new WpsProductionRequestConverter(requestReader).loadProductionRequest(getUserName());
                say(String.format("Production request loaded, type is '%s'.", request.getProductionType()));
            } finally {
                requestReader.close();
            }

            Production production = orderProduction(productionService, request);
            if (!production.isAutoStaging()) {
                stageProduction(productionService, production);
            }

        } catch (JDOMException e) {
            exit("Error: Invalid WPS XML", 3, e);
        } catch (ProductionException e) {
            exit("Error: Production failed", 4, e);
        } catch (IOException e) {
            exit("Error", 5, e);
        } catch (InterruptedException e) {
            exit("Warning: Workflow monitoring cancelled! Job may be still alive!", 0);
        } finally {
            if (productionService != null) {
                try {
                    productionService.close();
                } catch (Exception e) {
                    exit("Warning: Failed to close production service! Job may be still alive!", 0);
                }
            }
        }
    }

    private Production orderProduction(ProductionService productionService, ProductionRequest request) throws ProductionException, InterruptedException {
        say("Ordering production...");
        ProductionResponse productionResponse = productionService.orderProduction(request);
        Production production = productionResponse.getProduction();
        say("Production successfully ordered. The production ID is: " + production.getId());
        observeProduction(productionService, production);
        return production;
    }

    private void cancelProduction(ProductionService productionService, String productionId, Map<String, String> config) throws ProductionException, InterruptedException {
        if (productionId.startsWith("job_")) {
            say("Killing Hadoop job '" + productionId + "'...");
            Configuration hadoopConfig = new Configuration();
            HadoopProductionType.setJobConfig(hadoopConfig, config);
            try {
                JobClient jobClient = new JobClient(new JobConf(hadoopConfig));
                RunningJob job = jobClient.getJob(productionId);
                if (job != null) {
                    job.killJob();
                }
            } catch (IOException e) {
                exit("Failed to kill job", 100, e);
            }
        } else {
            say("Cancelling production '" + productionId + "'...");
            productionService.cancelProductions(productionId);
            Production production = productionService.getProduction(productionId);
            if (production != null) {
                observeProduction(productionService, production);
            } else {
                say("Warning: Production not found: " + productionId);
            }
        }
    }

    private void stageProduction(ProductionService productionService, Production production) throws ProductionException, InterruptedException {
        say("Staging results...");
        productionService.stageProductions(production.getId());
        observeStagingStatus(productionService, production);
    }

    private void observeStagingStatus(ProductionService productionService, Production production) throws InterruptedException {
        while (!production.getStagingStatus().isDone()) {
            Thread.sleep(500);
            productionService.updateStatuses();
            ProcessStatus stagingStatus = production.getStagingStatus();
            say(String.format("Staging status: state=%s, progress=%s, message='%s'",
                              stagingStatus.getState(),
                              stagingStatus.getProgress(),
                              stagingStatus.getMessage()));
        }
        if (production.getStagingStatus().getState() == ProcessState.COMPLETED) {
            say("Staging completed.");
        } else {
            exit("Error: Staging did not complete normally: " + production.getStagingStatus().getMessage(), 1);
        }
    }

    private void observeProduction(ProductionService productionService, Production production) throws InterruptedException {
        while (!production.getProcessingStatus().getState().isDone()) {
            Thread.sleep(1000);
            productionService.updateStatuses();
            ProcessStatus processingStatus = production.getProcessingStatus();
            say(String.format("Production remote status: state=%s, progress=%s, message='%s'",
                              processingStatus.getState(),
                              processingStatus.getProgress(),
                              processingStatus.getMessage()));
        }
        if (production.getProcessingStatus().getState() == ProcessState.COMPLETED) {
            say("Production completed. Output directory is " + production.getStagingPath());
        } else {
            exit("Error: Production did not complete normally: " + production.getProcessingStatus().getMessage(), 2);
        }
    }

    private void deployCalvalusSoftware(String sourcePathsString, Map<String, String> config) {
        final Path destinationPath = new Path(String.format("/calvalus/software/0.5/%s",
                                                            config.get("calvalus.calvalus.bundle")));
        Path[] sourcePaths = getSourcePaths(sourcePathsString);
        try {
            copyToHDFS(sourcePaths, destinationPath, config);
        } catch (IOException e) {
            exit("Error: Failed to deploy one or more files to Calvalus", 22, e);
        }
    }

    private void copyFilesToHDFS(String sourcePathsString, Map<String, String> config) {
        Path[] sourcePaths = getSourcePaths(sourcePathsString);
        final Path destinationPath = new Path(String.format("/calvalus/home/%s", getUserName()));
        try {
            copyToHDFS(sourcePaths, destinationPath, config);
        } catch (IOException e) {
            exit("Error: Failed to copy one or more files to HDFS", 23, e);
        }
    }

    private Path[] getSourcePaths(String sourcePathsString) {
        Path[] sourcePaths = toPathArray(sourcePathsString);
        ensureLocalPathsExist(sourcePaths);
        return sourcePaths;
    }

    private Path[] toPathArray(String pathsSpec) {
        String[] parts = pathsSpec.split(File.pathSeparator);
        Path[] paths = new Path[parts.length];
        for (int i = 0; i < parts.length; i++) {
            paths[i] = new Path(parts[i]);
        }
        return paths;
    }

    private void ensureLocalPathsExist(Path[] paths) {
        for (Path path : paths) {
            if (!new File(path.toString()).isFile()) {
                exit("Error: Local file does not exist: " + path, 21);
            }
        }
    }

    private void copyToHDFS(Path[] sourcePaths, Path destinationPath, Map<String, String> config) throws IOException {
        say("Copying " + sourcePaths.length + " local file(s) to HDFS...");
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("fs.default.name", config.get("calvalus.hadoop.fs.default.name"));
        FileSystem fs = FileSystem.get(hadoopConfig);
        Path qualifiedDestinationPath = fs.makeQualified(destinationPath);
        for (Path sourcePath : sourcePaths) {
            say("*  " + sourcePath + " --> " + qualifiedDestinationPath);
        }
        copyFromLocalFile(fs, sourcePaths, qualifiedDestinationPath);
        say("Files copied.");
    }

    private void copyFromLocalFile(FileSystem fs, Path[] sourcePaths, Path destinationDir) throws IOException {
        boolean overwrite = true;
        boolean delSrc = false;
        fs.mkdirs(destinationDir);
        fs.copyFromLocalFile(delSrc, overwrite, sourcePaths, destinationDir);
    }

    private void exit(String message, int exitCode, Throwable error) {
        System.err.println(message + ": " + error.getClass().getSimpleName() + ": " + error.getMessage());
        if (errors) {
            error.printStackTrace(System.err);
        }
        System.exit(exitCode);
    }

    private void say(String message) {
        if (!quite) {
            System.out.println(message);
        }
    }

    private void exit(String message, int exitCode) {
        System.err.println(message);
        System.exit(exitCode);
    }

    private void printHelp() {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(TOOL_NAME + " [OPTION]... REQUEST",
                                "\nThe Calvalus production tool submits a production REQUEST to a Calvalus production system. REQUEST must be a plain text XML file " +
                                        "conforming to the WPS Execute operation request (see http://schemas.opengis.net/wps/1.0.0/wpsExecute_request.xsd). OPTION may be one or more of the following:",
                                TOOL_OPTIONS,
                                "", false);
    }

    public CommandLine parseCommandLine(String... args) throws ParseException {
        Parser parser = new GnuParser();
        return parser.parse(TOOL_OPTIONS, args);
    }

    @SuppressWarnings({"AccessStaticViaInstance"})
    public static Options createCommandlineOptions() {
        Options options = new Options();
        options.addOption(OptionBuilder
                                  .withLongOpt("quite")
                                  .withDescription("Quite mode, only minimum console output.")
                                  .create("q"));
        options.addOption(OptionBuilder
                                  .withLongOpt("errors")
                                  .withDescription("Print full Java stack trace on exceptions.")
                                  .create("e"));
        options.addOption(OptionBuilder
                                  .withLongOpt("help")
                                  .withDescription("Prints out usage help.")
                                  .create()); // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("calvalus")
                                  .hasArg()
                                  .withArgName("NAME")
                                  .withDescription("The name of the Calvalus software bundle used for the production. Defaults to '" + DEFAULT_CALVALUS_BUNDLE + "'")
                                  .create("C"));
        options.addOption(OptionBuilder
                                  .withLongOpt("beam")
                                  .hasArg()
                                  .withArgName("NAME")
                                  .withDescription("The name of the BEAM software bundle used for the production. Defaults to '" + DEFAULT_BEAM_BUNDLE + "'.")
                                  .create("B"));
        options.addOption(OptionBuilder
                                  .withLongOpt("config")
                                  .hasArg()
                                  .withArgName("FILE")
                                  .withDescription("The Calvalus configuration file (Java properties format). Defaults to '" + DEFAULT_CONFIG_PATH + "'.")
                                  .create("c"));
        options.addOption(OptionBuilder
                                  .withLongOpt("deploy")
                                  .hasArg()
                                  .withArgName("FILES")
                                  .withDescription("Deploys FILES to the Calvalus bundle before the request is executed. " +
                                                           "The Calvalus bundle given by the 'calvalus' option. " +
                                                           "Use the colon ':' to separate multiple paths in FILES.")
                                  .create());  // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("copy")
                                  .hasArgs()
                                  .withArgName("FILES")
                                  .withDescription("Copies FILES to '/calvalus/home/<user>' before the request is executed." +
                                                           "Use the colon ':' to separate paths in SOURCES.")
                                  .create());  // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("kill")
                                  .hasArgs()
                                  .withArgName("PID")
                                  .withDescription("Kills the production with given identifier PID.")
                                  .create());  // (sub) commands don't have short options
        return options;
    }

    private static String getUserName() {
        return System.getProperty("user.name", "anonymous").toLowerCase();
    }


}
