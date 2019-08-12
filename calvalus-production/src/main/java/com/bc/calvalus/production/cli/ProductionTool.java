package com.bc.calvalus.production.cli;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.ingestion.IngestionTool;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopJobHook;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.ProcessingLogHandler;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionResponse;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceConfig;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.production.hadoop.HadoopProductionType;
import com.bc.calvalus.production.hadoop.HadoopServiceContainerFactory;
import com.bc.calvalus.production.util.CasUtil;
import com.bc.calvalus.production.util.DebugTokenGenerator;
import com.bc.calvalus.production.util.TokenGenerator;
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
import org.apache.xml.security.Init;
import org.apache.xml.security.encryption.XMLCipher;
import org.apache.xml.security.encryption.XMLEncryptionException;
import org.apache.xml.security.utils.EncryptionConstants;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.esa.snap.core.util.StringUtils;
import org.jdom.JDOMException;
import org.jdom2.Element;
import org.jdom2.input.DOMBuilder;
import org.jdom2.output.DOMOutputter;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.crypto.Cipher;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static com.bc.calvalus.production.ProcessingLogHandler.LOG_STREAM_EMPTY_ERROR_CODE;

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
 *                         the production. Defaults to 'calvalus-2.19-SNAPSHOT'
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
    private static final String DEFAULT_AUTH_METHOD = "unix";
    private static final String CALVALUS_SOFTWARE_HOME = HadoopProcessingService.CALVALUS_SOFTWARE_PATH;

    private static final String BUNDLE_SEPARATOR = "-->";

    private static final String TOOL_NAME = "cpt";
    private static final String SYSTEM_USER_NAME = "hadoop";
    private static final Options TOOL_OPTIONS = createCommandlineOptions();
    private static final String DEFAULT_LOG_DIRECTORY = "log/";
    private static final String JOB_REPORT_FILE = "job.report";
    private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private boolean errors;
    private boolean quiet;

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

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
                || commandLine.hasOption("help")
                || commandLine.hasOption("test-auth");
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
        defaultConfig.put("calvalus.crypt.auth", "unix");
        defaultConfig.put("calvalus.crypt.calvalus-public-key", "/opt/hadoop/conf/calvalus_pub.der");
        defaultConfig.put("calvalus.crypt.debug-private-key", "/opt/hadoop/conf/debug_priv.der");
        defaultConfig.put("calvalus.crypt.debug-certificate", "/opt/hadoop/conf/debug_cert.der");

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

            if (commandLine.hasOption("test-auth")) {
                String samlToken = new CasUtil(quiet).fetchSamlToken(config, getUserName());
                say("Successfully retrieved SAML token:\n");
                say(samlToken);
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
            ProductionRequest request;
            try (FileReader requestReader = new FileReader(requestPath)) {
                say(String.format("Loading production request '%s'...", requestPath));
                if (requestPath.endsWith(".yaml") || requestPath.endsWith(".yml")) {
                    say("Production request  format is 'YAML'");
                    request = new YamlProductionRequestConverter(requestReader).loadProductionRequest(getUserName());
                } else {
                    say("Production request  format is 'WPS-XML'");
                    request = new WpsProductionRequestConverter(requestReader).loadProductionRequest(getUserName());
                }
                say(String.format("Production request loaded, type is '%s'.", request.getProductionType()));
            }

            String authPolicy;
            if (commandLine.hasOption("auth")) {
                authPolicy = commandLine.getOptionValue("auth");
            } else {
                authPolicy = config.getOrDefault("calvalus.crypt.auth", "unix");
            }
            HadoopJobHook hook = null;
            switch (authPolicy) {
                case "unix":
                    break;
                case "debug":
                    String publicKey = config.get("calvalus.crypt.calvalus-public-key");
                    String privateKey = config.get("calvalus.crypt.debug-private-key");
                    String certificate = config.get("calvalus.crypt.debug-certificate");
                    hook = new DebugTokenGenerator(publicKey, privateKey, certificate, getUserName());
                    break;
                case "saml":
                    String samlToken = new CasUtil(quiet).fetchSamlToken(config, getUserName()).replace("\\s+", "");
                    String publicKeySaml = config.get("calvalus.crypt.calvalus-public-key");
                    hook = new TokenGenerator(publicKeySaml, samlToken);
                    break;
                default:
                    throw new RuntimeException("unknown auth type " + authPolicy);
            }

            String jobSubmissionDate = df.format(new Date());
            config.put("jobSubmissionDate", jobSubmissionDate);

            String systemName = config.get(JobConfigNames.CALVALUS_SYSTEM_NAME);
            if(StringUtils.isNotNullAndNotEmpty(systemName)){
                request.setParameter(JobConfigNames.CALVALUS_SYSTEM_NAME, systemName);
            }

            Production production = orderProduction(serviceContainer.getProductionService(), request, hook);
            if (production.isAutoStaging()) {
                stageProduction(serviceContainer.getProductionService(), production);
            }
            if (commandLine.hasOption("joblogs")){
                handleJobLogs(commandLine, serviceContainer, config, production);
            }

        } catch (JDOMException e) {
            exit("Error: Invalid WPS XML", 3, e);
        } catch (ProductionException e) {
            exit("Error: Production failed", 4, e);
        } catch (IOException e) {
            exit("Error", 5, e);
        } catch (InterruptedException e) {
            exit("Warning: Workflow monitoring cancelled! Job may be still alive!", 0);
        } catch (GeneralSecurityException e) {
            exit("Error fetching SAML token.", 6, e);
        } catch (ParserConfigurationException | SAXException | org.jdom2.JDOMException e) {
            exit("SAML is not well-formed.", 7, e);
        } catch (XMLEncryptionException e) {
            exit("Unable to decipher SAML token.", 8, e);
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

    private void handleJobLogs(CommandLine commandLine, ServiceContainer serviceContainer, Map<String, String> config,
                               Production production) throws IOException {
        boolean withExternalAccessControl = serviceContainer.getHadoopConfiguration().getBoolean(
                    "calvalus.accesscontrol.external", false);
        ProcessingLogHandler logHandler = new ProcessingLogHandler(config, withExternalAccessControl);
        String logFilePath = commandLine.getOptionValue("joblogs");
        OutputStream out = null;
        try {
            if ("stdout".equalsIgnoreCase(logFilePath)) {
                out = new ByteArrayOutputStream();
                int responseCode = logHandler.handleProduction(production, out, getUserName());
                if (responseCode == 0) {
                    System.out.println(out.toString());
                } else {
                    System.out.println(
                                "There is an issue in accessing the log for this productionId '" + production.getId() + "'");
                }
            } else {
                java.nio.file.Path logDirPath = Paths.get(DEFAULT_LOG_DIRECTORY);
                if (!Files.exists(logDirPath)) {
                    System.out.println("Creating '" + DEFAULT_LOG_DIRECTORY + "' directory");
                    Files.createDirectory(logDirPath);
                }
                if (StringUtils.isNullOrEmpty(logFilePath)) {
                    logFilePath = DEFAULT_LOG_DIRECTORY + production.getId() + ".log";
                }
                out = new FileOutputStream(logFilePath);
                System.out.println("Writing processing logs....");
                int responseCode = logHandler.handleProduction(production, out, getUserName());
                if (responseCode == 0) {
                    System.out.println("Log has been successfully written to '" + logFilePath + "'");
                } else if (responseCode == LOG_STREAM_EMPTY_ERROR_CODE) {
                    System.out.println("Log files return no contents. It is likely that log aggregation process " +
                                       "has not been completed. Try again later with command 'yarn logs -applicationId " +
                                       "<applicationId>. Check " + DEFAULT_LOG_DIRECTORY + JOB_REPORT_FILE + " for " +
                                       "information about the applicationId");
                    java.nio.file.Path logPath = Paths.get(logFilePath);
                    if(Files.exists(logPath)){
                        Files.delete(logPath);
                    }
                } else {
                    System.out.println(
                                "There is an issue in accessing the log for this process. Please see '" + logFilePath + "'");
                }
                Object[] jobIds = production.getJobIds();
                for (Object jobId : jobIds) {
                    String newLine = config.get("jobSubmissionDate") + "\t" +
                                     (jobId.toString().replace("job", "application")) + "\t" +
                                     production.getName() + "\n";
                    try {
                        java.nio.file.Path jobReportPath = Paths.get(DEFAULT_LOG_DIRECTORY, JOB_REPORT_FILE);
                        Files.write(jobReportPath, newLine.getBytes(),
                                    Files.exists(jobReportPath) ? StandardOpenOption.APPEND : StandardOpenOption.CREATE);
                    } catch (IOException e) {
                        System.err.println("Unable to add new job to " + DEFAULT_LOG_DIRECTORY + JOB_REPORT_FILE);
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }

    private Production orderProduction(ProductionService productionService, ProductionRequest request, HadoopJobHook hook) throws
            ProductionException,
            InterruptedException {
        say("Ordering production...");
        ProductionResponse productionResponse = productionService.orderProduction(request, hook);
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
//        if (config.containsKey("calvalus.hadoop.systemuser")) {
//            username = config.get("calvalus.hadoop.systemuser");
//        }
//        UserGroupInformation hadoop = UserGroupInformation.createRemoteUser(username);
//        try {
//            return hadoop.doAs(new PrivilegedExceptionAction<FileSystem>() {
//                @Override
//                public FileSystem run() throws Exception {
        Configuration hadoopConfig = getHadoopConf(config);
        // this get the faulf FS, which is HDFS
        return FileSystem.get(hadoopConfig);
//                }
//            });
//        } catch (InterruptedException e) {
//            throw new IOException("Interrupted:", e);
//        }
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
                .withLongOpt("auth")
                .hasArg()
                .withArgName("NAME")
                .withDescription(
                        "Authentication method. One of unix, saml, debug. Defaults to '" + DEFAULT_AUTH_METHOD + "'.")
                .create("a"));
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
                .withDescription("Product type of uploaded files, defaults to " + IngestionTool.DEFAULT_PRODUCT_TYPE + ", not set for pathtemplate ingestion")
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
                .create());
        options.addOption(OptionBuilder
                .withLongOpt("test-auth")
                .withDescription("Test authentication by SAML token. Print SAML token on success.")
                .create());  // (sub) commands don't have short options
        options.addOption(OptionBuilder
                .withLongOpt("joblogs")
                .withDescription("Store the aggregated logs in FILEPATH. When FILEPATH is not specified, the logs are stored " +
                                 "in " + DEFAULT_LOG_DIRECTORY + " directory. Enter 'stdout' to display the logs in standard output.")
                .hasOptionalArg()
                .withArgName("FILEPATH")
                .create());  // (sub) commands don't have short options
        return options;
    }

    private static String getUserName() {
        return System.getProperty("user.name", "anonymous");
    }
}
