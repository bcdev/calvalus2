package com.bc.calvalus.production.cli;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.ingestion.IngestionTool;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionResponse;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceConfig;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.production.hadoop.HadoopServiceContainerFactory;
import com.bc.calvalus.production.hadoop.HadoopProductionType;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.security.UserGroupInformation;
import org.jdom.JDOMException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
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
 *  -S,--snap &lt;NAME&gt;       The name of the SNAP software bundle used for the
 *                         production. Defaults to 'snap-2.0.0'.
 *  -c,--config &lt;FILE&gt;     The Calvalus configuration file (Java properties
 *                         format). Defaults to 'C:\Users\Norman\.calvalus\calvalus.config'.
 *  -C,--calvalus &lt;NAME&gt;   The name of the Calvalus software bundle used for
 *                         the production. Defaults to 'calvalus-2.14-SNAPSHOT'
 *     --copy &lt;FILES&gt;      Copies FILES to '/calvalus/home/&lt;user&gt;' before the
 *                         request is executed.Use the colon ':' to separate paths in FILES.
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

    private static final String DEFAULT_CONFIG_PATH = new File(ProductionServiceConfig.getUserAppDataDir(),
                                                               "calvalus.config").getPath();

    private static final String DEFAULT_SNAP_BUNDLE = HadoopProcessingService.DEFAULT_SNAP_BUNDLE;
    private static final String DEFAULT_CALVALUS_BUNDLE = HadoopProcessingService.DEFAULT_CALVALUS_BUNDLE;
    private static final String CALVALUS_SOFTWARE_HOME = HadoopProcessingService.CALVALUS_SOFTWARE_PATH;

    private static final String BUNDLE_SEPARATOR = "-->";

    private static final String TOOL_NAME = "cpt";
    private static final String SYSTEM_USER_NAME = "hadoop";
    private static final Options TOOL_OPTIONS = createCommandlineOptions();

    private boolean errors;
    private boolean quiet;

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
                                  || commandLine.hasOption("uninstall")
                                  || commandLine.hasOption("install")
                                  || commandLine.hasOption("kill")
                                  || commandLine.hasOption("copy")
                                  || commandLine.hasOption("ingestion")
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
        quiet = commandLine.hasOption("quiet");

        if (commandLine.hasOption("help")) {
            printHelp();
            return;
        }

        Map<String, String> defaultConfig = ProductionServiceConfig.getCalvalusDefaultConfig();
        defaultConfig.put("production.db.type", "memory");

        defaultConfig.put("calvalus.calvalus.bundle", commandLine.getOptionValue("calvalus", DEFAULT_CALVALUS_BUNDLE));
        defaultConfig.put("calvalus.snap.bundle", commandLine.getOptionValue("snap", DEFAULT_SNAP_BUNDLE));

        ServiceContainer serviceContainer = null;
        try {
            String configFile = commandLine.getOptionValue("config", DEFAULT_CONFIG_PATH);
            say(String.format("Loading Calvalus configuration '%s'...", configFile));
            Map<String, String> config = ProductionServiceConfig.loadConfig(new File(configFile), defaultConfig);
            say("Configuration loaded.");

            if (commandLine.hasOption("ingestion")) {
                IngestionTool.handleIngestionCommand(commandLine, commandLine.getOptionValues("ingestion"), getHDFS(SYSTEM_USER_NAME, config));
                return;
            }

            if (commandLine.hasOption("uninstall")) {
                uninstallBundles(commandLine.getOptionValue("uninstall"), config);
            }

            if (commandLine.hasOption("install")) {
                installBundles(commandLine.getOptionValue("install"), config);
            }

            if (commandLine.hasOption("deploy")) {
                deployBundleFiles(commandLine.getOptionValues("deploy"), config);
            }

            if (commandLine.hasOption("copy")) {
                copyFilesToHDFS(commandLine.getOptionValue("copy"), config);
            }

            // Don't exit if we want to 'kill' something, since we need the production service to do so.
            if (requestPath == null && !commandLine.hasOption("kill")) {
                return;
            }

            HadoopServiceContainerFactory productionServiceFactory = new HadoopServiceContainerFactory();
            serviceContainer = productionServiceFactory.create(config, ProductionServiceConfig.getUserAppDataDir(),
                                                                                new File("."));

            if (commandLine.hasOption("kill")) {
                cancelProduction(serviceContainer.getProductionService(), commandLine.getOptionValue("kill"), config);
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
                if (requestPath.endsWith(".yaml")||requestPath.endsWith(".yml")) {
                    say("Production request  format is 'YAML'");
                    request = new YamlProductionRequestConverter(requestReader).loadProductionRequest(getUserName());
                } else {
                    say("Production request  format is 'WPS-XML'");
                    request = new WpsProductionRequestConverter(requestReader).loadProductionRequest(getUserName());
                }
                say(String.format("Production request loaded, type is '%s'.", request.getProductionType()));
            } finally {
                requestReader.close();
            }

            Production production = orderProduction(serviceContainer.getProductionService(), request);
            if (production.isAutoStaging()) {
                stageProduction(serviceContainer.getProductionService(), production);
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
            if (serviceContainer != null && serviceContainer.getProductionService() != null) {
                try {
                    serviceContainer.getProductionService().close();
                } catch (Exception e) {
                    exit("Warning: Failed to close production service! Job may be still alive!", 0);
                }
            }
        }
    }

    private Production orderProduction(ProductionService productionService, ProductionRequest request) throws
            ProductionException,
            InterruptedException {
        say("Ordering production...");
        ProductionResponse productionResponse = productionService.orderProduction(request);
        Production production = productionResponse.getProduction();
        say("Production successfully ordered. The production ID is: " + production.getId());
        observeProduction(productionService, production);
        return production;
    }

    private void cancelProduction(ProductionService productionService, String productionId,
                                  Map<String, String> config) throws ProductionException, InterruptedException {
        if (productionId.startsWith("job_")) {
            say("Killing Hadoop job '" + productionId + "'...");
            Configuration hadoopConfig = new Configuration();
            HadoopProductionType.setJobConfig(config, hadoopConfig);
            try {
                JobClient jobClient = new JobClient(new JobConf(hadoopConfig));
                RunningJob job = jobClient.getJob(JobID.forName(productionId));
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

    private void stageProduction(ProductionService productionService, Production production) throws ProductionException,
            InterruptedException {
        say("Staging results...");
        productionService.stageProductions(production.getId());
        observeStagingStatus(productionService, production);
    }

    private void observeStagingStatus(ProductionService productionService, Production production) throws
            InterruptedException {
        String userName = production.getProductionRequest().getUserName();
        while (!production.getStagingStatus().isDone()) {
            Thread.sleep(500);
            productionService.updateStatuses(userName);
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

    private void observeProduction(ProductionService productionService, Production production) throws
            InterruptedException {
        final Thread shutDownHook = createShutdownHook(production.getWorkflow());
        Runtime.getRuntime().addShutdownHook(shutDownHook);

        String userName = production.getProductionRequest().getUserName();
        while (!production.getProcessingStatus().getState().isDone()) {
            Thread.sleep(5000);
            productionService.updateStatuses(userName);
            ProcessStatus processingStatus = production.getProcessingStatus();
            say(String.format("Production remote status: state=%s, progress=%s, message='%s'",
                              processingStatus.getState(),
                              processingStatus.getProgress(),
                              processingStatus.getMessage()));
        }
        Runtime.getRuntime().removeShutdownHook(shutDownHook);

        if (production.getProcessingStatus().getState() == ProcessState.COMPLETED) {
            say("Production completed. Output directory is " + production.getStagingPath());
        } else {
            exit("Error: Production did not complete normally: " + production.getProcessingStatus().getMessage(), 2);
        }
    }

    private Thread createShutdownHook(final WorkflowItem workflow) {
        return new Thread() {
            @Override
            public void run() {
                try {
                    workflow.kill();
                } catch (Exception e) {
                    say("Failed to shutdown production: " + e.getMessage());
                }
            }
        };
    }

    private void installBundles(String sourcePathsString, Map<String, String> config) {
        Path[] sourcePaths = getSourcePaths(sourcePathsString);
        try {
            installBundles0(sourcePaths, config);
        } catch (IOException e) {
            exit("Error: Failed to install one or more bundles on Calvalus", 22, e);
        }
    }

    private void uninstallBundles(String uninstallArg, Map<String, String> config) {
        String[] bundleNames = uninstallArg.split(",");
        try {
            uninstallBundles0(bundleNames, config);
        } catch (IOException e) {
            exit("Error: Failed to uninstall one or more bundles from Calvalus", 22, e);
        }
    }

    private void deployBundleFiles(String[] deployArgs, final Map<String, String> config) {
        final String bundleName;
        final Path[] sourcePaths;
        if (deployArgs.length == 1) {
            String deployArg = deployArgs[0];
            int pos = deployArg.lastIndexOf(BUNDLE_SEPARATOR);
            if (pos < 0 || pos == deployArg.length() - BUNDLE_SEPARATOR.length()) {
                exit("Error: Failed to deploy bundle files: no bundle name given: " + deployArg, 22);
            } else if (pos == 0) {
                exit("Error: Failed to deploy bundle files: no files given: " + deployArg, 22);
            }
            bundleName = deployArg.substring(pos + BUNDLE_SEPARATOR.length());
            String sourcePathsString = deployArg.substring(0, pos);
            sourcePaths = getSourcePaths(sourcePathsString);
        } else {
            bundleName = deployArgs[deployArgs.length - 1];
            sourcePaths = new Path[deployArgs.length - 1];
            for (int i = 0; i < deployArgs.length - 1; i++) {
                sourcePaths[i] = new Path(deployArgs[i]);
            }
            ensureLocalPathsExist(sourcePaths);
        }
        try {
            FileSystem fs = getHDFS(SYSTEM_USER_NAME, config);
            copy(sourcePaths, fs, getQualifiedBundlePath(fs, bundleName));
        } catch (IOException e) {
            exit("Error: Failed to deploy one or more bundle files", 22, e);
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
            if (!new File(path.toString()).exists()) {
                exit("Error: Local file does not exist: " + path, 21);
            }
        }
    }

    private void installBundles0(Path[] sourcePaths, Map<String, String> config) throws IOException {
        FileSystem hdfs = getHDFS(SYSTEM_USER_NAME, config);
        say("Installing " + sourcePaths.length + " bundle(s) in '" + getQualifiedSoftwareHome(hdfs) + "'...");
        for (Path sourcePath : sourcePaths) {
            String bundleName = getBundleName(sourcePath);
            Path bundlePath = getQualifiedBundlePath(hdfs, bundleName);
            uninstallBundle(hdfs, bundlePath, false);
            installBundle(sourcePath, hdfs, bundlePath, hdfs.getConf());
        }
        say(sourcePaths.length + " bundles installed.");
    }

    private void uninstallBundles0(String[] bundleNames, Map<String, String> config) throws IOException {
        FileSystem hdfs = getHDFS(SYSTEM_USER_NAME, config);
        say("Uninstalling " + bundleNames.length + " bundle(s) from '" + getQualifiedSoftwareHome(hdfs) + "'...");
        for (String bundleName : bundleNames) {
            Path bundlePath = getQualifiedBundlePath(hdfs, bundleName);
            uninstallBundle(hdfs, bundlePath, true);
        }
        say(bundleNames.length + " bundle(s) uninstalled.");
    }

    private void installBundle(Path sourcePath, FileSystem hdfs, Path bundlePath, Configuration hadoopConfig) throws
            IOException {
        DirCopy.copyDir(new File(sourcePath.toString()), hdfs, bundlePath, hadoopConfig);
        say("+ " + bundlePath.getName() + " installed");
    }

    private void uninstallBundle(FileSystem hdfs, Path qualifiedDestinationPath, boolean mustExist) throws IOException {
        if (hdfs.exists(qualifiedDestinationPath)) {
            if (hdfs.delete(qualifiedDestinationPath, true)) {
                say("- " + qualifiedDestinationPath.getName() + " uninstalled");
            } else {
                say("! " + qualifiedDestinationPath.getName() + " could not be deleted");
            }
        } else if (mustExist) {
            say("! " + qualifiedDestinationPath.getName() + " does not exists");
        }
    }

    private FileSystem getHDFS(String username, final Map<String, String> config) throws IOException {
        if (config.containsKey("calvalus.hadoop.systemuser")) {
            username = config.get("calvalus.hadoop.systemuser");
        }
        UserGroupInformation hadoop = UserGroupInformation.createRemoteUser(username);
        try {
            return hadoop.doAs(new PrivilegedExceptionAction<FileSystem>() {
                @Override
                public FileSystem run() throws Exception {
                    Configuration hadoopConfig = getHadoopConf(config);
                    // this get the faulf FS, which is HDFS
                    return FileSystem.get(hadoopConfig);
                }
            });
        } catch (InterruptedException e) {
            throw new IOException("Interrupted:", e);
        }
    }

    private void copyToHDFS(Path[] sourcePaths, Path destinationPath, Map<String, String> config) throws IOException {
        copy(sourcePaths, getHDFS(getUserName(), config), destinationPath);
    }

    private void copy(Path[] sourcePaths, FileSystem fs, Path destinationPath) throws IOException {
        say("Copying " + sourcePaths.length + " local file(s) to " + destinationPath + "...");
        Path qualifiedDestinationPath = fs.makeQualified(destinationPath);
        for (Path sourcePath : sourcePaths) {
            say("+ " + sourcePath.getName());
        }
        copyFromLocalFile(fs, sourcePaths, qualifiedDestinationPath);
        say(sourcePaths.length + " file(s) copied.");
    }

    private void copyFromLocalFile(FileSystem fs, Path[] sourcePaths, Path destinationDir) throws IOException {
        boolean overwrite = true;
        boolean delSrc = false;
        fs.mkdirs(destinationDir);
        fs.copyFromLocalFile(delSrc, overwrite, sourcePaths, destinationDir);
    }

    private Configuration getHadoopConf(Map<String, String> config) {
        Configuration hadoopConfig = new Configuration();
        for (String key : config.keySet()) {
            if (key.startsWith("calvalus.hadoop.")) {
                hadoopConfig.set(key.substring("calvalus.hadoop.".length()), config.get(key));
            }
        }
        return hadoopConfig;
    }

    private Path getQualifiedBundlePath(FileSystem fs, String bundleName) {
        char[] invalidCharacters = {'/', ':', ';', ','};
        for (char invalidCharacter : invalidCharacters) {
            if (bundleName.indexOf(invalidCharacter) != -1) {
                exit("Error: Invalid bundle name: " + bundleName, 22);
            }
        }
        return new Path(getQualifiedSoftwareHome(fs), bundleName);
    }

    private Path getQualifiedSoftwareHome(FileSystem fs) {
        return fs.makeQualified(new Path(CALVALUS_SOFTWARE_HOME));
    }

    private String getBundleName(Path sourcePath) {
        String bundleName = sourcePath.getName();
        if (bundleName.toLowerCase().endsWith(".zip")) {
            return bundleName.substring(0, bundleName.length() - ".zip".length());
        }
        if (bundleName.toLowerCase().endsWith(".jar")) {
            return bundleName.substring(0, bundleName.length() - ".jar".length());
        }
        return bundleName;
    }

    private void exit(String message, int exitCode, Throwable error) {
        System.err.println(message + ": " + error.getClass().getSimpleName() + ": " + error.getMessage());
        if (errors) {
            error.printStackTrace(System.err);
        }
        System.exit(exitCode);
    }

    private void say(String message) {
        if (!quiet) {
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
                                  .withLongOpt("quiet")
                                  .withDescription("Quiet mode, only minimum console output.")
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
                                  .withDescription(
                                          "The name of the Calvalus software bundle used for the production. Defaults to '" + DEFAULT_CALVALUS_BUNDLE + "'.")
                                  .create("C"));
        options.addOption(OptionBuilder
                                  .withLongOpt("snap")
                                  .hasArg()
                                  .withArgName("NAME")
                                  .withDescription(
                                          "The name of the SNAP software bundle used for the production. Defaults to '" + DEFAULT_SNAP_BUNDLE + "'.")
                                  .create("S"));
        options.addOption(OptionBuilder
                                  .withLongOpt("config")
                                  .hasArg()
                                  .withArgName("FILE")
                                  .withDescription(
                                          "The Calvalus configuration file (Java properties format). Defaults to '" + DEFAULT_CONFIG_PATH + "'.")
                                  .create("c"));
        options.addOption(OptionBuilder
                                  .withLongOpt("copy")
                                  .hasArgs()
                                  .withArgName("FILES")
                                  .withDescription(
                                          "Copies FILES to '/calvalus/home/<user>' before any request is executed." +
                                          "Use character '" + File.pathSeparator + "' to separate paths in FILES.")
                                  .create());  // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("deploy")
                                  .hasArgs()
                                  .withArgName("FILES-->BUNDLE")
                                  .withDescription(
                                          "Deploys FILES (usually JARs) to the Calvalus BUNDLE before any request is executed. " +
                                          "Use the character string '-->' to separate list of FILES from BUNDLE name. " +
                                          "Use character '" + File.pathSeparator + "' to separate multiple paths in FILES. " +
                                          "Alternatively a list of files and as last argument the bundle name can be given.")
                                  .create());  // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("install")
                                  .hasArgs()
                                  .withArgName("BUNDLES")
                                  .withDescription("Installs list of BUNDLES (directories, ZIP-, or JAR-files) " +
                                                   "on Calvalus before any request is executed." +
                                                   "Use character '" + File.pathSeparator + "' to separate multiple entries in BUNDLES.")
                                  .create());  // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("uninstall")
                                  .hasArgs()
                                  .withArgName("BUNDLES")
                                  .withDescription("Uninstalls list of BUNDLES (directories or ZIP-files) " +
                                                   "from Calvalus before any request is executed." +
                                                   "Use character ',' to separate multiple entries in BUNDLES.")
                                  .create());  // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("kill")
                                  .hasArgs()
                                  .withArgName("PID")
                                  .withDescription("Kills the production with given identifier PID.")
                                  .create());  // (sub) commands don't have short options


        options.addOption(OptionBuilder
                                  .withLongOpt("ingestion")
                                  .hasArgs()
                                  .withArgName("FILES")
                                  .withDescription("Transfers EO data products to HDFS.")
                                  .create());  // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("producttype")
                                  .hasArg()
                                  .withDescription("Product type of uploaded files, defaults to " + IngestionTool.DEFAULT_PRODUCT_TYPE+ ", not set for pathtemplate ingestion")
                                  .create());  // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("revision")
                                  .hasArg()
                                  .withDescription("Revision of uploaded files, defaults to " + IngestionTool.DEFAULT_REVISION + ", not set for pathtemplate ingestion")
                                  .create());  // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("replication")
                                  .hasArg()
                                  .withDescription("Replication factor of uploaded files, defaults 1")
                                  .create());  // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("blocksize")
                                  .hasArg()
                                  .withDescription("Block size in MB for uploaded files, defaults to file size")
                                  .create());  // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("filenamepattern")
                                  .hasArg()
                                  .withDescription("Regular expression matching filenames or paths below ingestion dir, defaults to 'producttype.*\\.N1'")
                                  .create());  // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("timeelements")
                                  .hasArg()
                                  .withDescription("match groups composed to a date string according to timeformat, e.g. '\\1\\2\\3', not set for canonical type-and-revision ingestion")
                                  .create());  // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("timeformat")
                                  .hasArg()
                                  .withDescription("SimpleDateFormat pattern, e.g. yyyyMMdd, not set for canonical type-and-revision ingestion")
                                  .create());  // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("pathtemplate")
                                  .hasArg()
                                  .withDescription("3 cases: SimpleDateFormat pattern (e.g. '/calvalus/eodata/MER_RR__1P/r03/'yyyy'/'MM'/'dd) if timeelements and timeformat are provided," +
                                                           " path element template (e.g. '/calvalus/eodata/MER_RR__1P/r03/\\1/\\2/\\3') if timeelements and timeformat are not provided," +
                                                           " not set for canonical type-and-revision ingestion")
                                  .create());  // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("verify")
                                  .withDescription("Verify existence and size to avoid double copying, defaults to false")
                                  .create());  // (sub) commands don't have short options
        return options;
    }

    private static String getUserName() {
        return System.getProperty("user.name", "anonymous").toLowerCase();
    }


}
