package com.bc.calvalus.production.cli;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.hadoop.HadoopJobHook;
import com.bc.calvalus.production.util.CasUtil;
import com.bc.calvalus.production.util.DebugTokenGenerator;
import com.bc.calvalus.production.util.TokenGenerator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.xml.security.encryption.XMLEncryptionException;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class CalvalusHadoopTool {

    private static final int STATUS_POLLING_INTERVAL_MILLIS = 5000;
    private static Logger LOG = CalvalusLogger.getLogger();

    private final CalvalusHadoopConnection hadoopConnection;
    private final CalvalusHadoopRequestConverter requestConverter;
    private final CalvalusHadoopStatusConverter statusConverter;

    public CalvalusHadoopTool(String userName, String format) {
        hadoopConnection = new CalvalusHadoopConnection(userName);
        requestConverter = new CalvalusHadoopRequestConverter(hadoopConnection, userName);
        //statusConverter = new CalvalusHadoopStatusConverter(hadoopConnection);
        statusConverter = CalvalusHadoopStatusConverter.create(hadoopConnection, format);
    }

    public static void main(String[] args) {
        CommandLine commandLine = null;
        try {
            Options options = createCommandLineOptions();
            commandLine = new GnuParser().parse(options, args);
            Properties configParameters = CalvalusHadoopRequestConverter.collectConfigParameters(commandLine);
            String userName = System.getProperty("user.name");
            if ("boe".equals(userName)) { userName = "martin"; }  // TODO remove

            if (commandLine.hasOption("quiet")) {
                LOG.setLevel(Level.SEVERE);
            } else if (commandLine.hasOption("status")) {
                LOG.setLevel(Level.SEVERE);
            } else if (commandLine.hasOption("debug")) {
                LOG.setLevel(Level.FINER);
            } else {
                LOG.setLevel(Level.FINE);
            }

            if (commandLine.hasOption("help")) {

                new HelpFormatter().printHelp("cht [OPTION]... REQUEST|IDs",
                                              "\nThe Calvalus Hadoop Tool translates a request into a Hadoop job, submits it, inquires status. OPTION may be one or more of the following:",
                                              options,
                                              "", false);

            } else if (commandLine.hasOption("test-auth")) {

                String samlToken = new CasUtil(false).fetchSamlToken2(configParameters, userName);
                System.out.println("Successfully retrieved SAML token:\n");
                System.out.println(samlToken);

            } else if ((commandLine.hasOption("status") || commandLine.hasOption("cancel")) && commandLine.getArgList().size() < 1) {

                System.err.println("Error: At least one argument JOBID expected. (use option --help for usage help)");
                System.exit(1);

            } else if (commandLine.hasOption("status")) {

                String[] ids = commandLine.getArgs();
                Map<String, String> commandLineParameters = CalvalusHadoopRequestConverter.collectCommandLineParameters(commandLine);
                final CalvalusHadoopTool calvalusHadoopTool = new CalvalusHadoopTool(userName, commandLine.getOptionValue("format", "json"));
                if (! calvalusHadoopTool.getStatus(userName, ids, commandLineParameters, configParameters)) {
                    System.exit(1);
                }

            } else if (commandLine.hasOption("cancel")) {

                String[] ids = commandLine.getArgs();
                Map<String, String> commandLineParameters = CalvalusHadoopRequestConverter.collectCommandLineParameters(commandLine);
                final CalvalusHadoopTool calvalusHadoopTool = new CalvalusHadoopTool(userName, commandLine.getOptionValue("format", "json"));
                if (! calvalusHadoopTool.cancel(userName, ids, commandLineParameters, configParameters)) {
                    System.exit(1);
                }

            } else if (commandLine.getArgList().size() != 1) {

                System.err.println("Error: One argument REQUEST expected. (use option --help for usage help)");
                System.exit(1);

            } else {

                String requestPath = String.valueOf(commandLine.getArgList().get(0));
                boolean overwriteOutput = commandLine.hasOption("overwrite");
                Map<String, String> commandLineParameters = CalvalusHadoopRequestConverter.collectCommandLineParameters(commandLine);
                final CalvalusHadoopTool calvalusHadoopTool = new CalvalusHadoopTool(userName, commandLine.getOptionValue("format", "json"));
                RunningJob runningJob = calvalusHadoopTool.exec(userName, requestPath, commandLineParameters, configParameters, overwriteOutput);

                if (commandLine.hasOption("async")) {
                    System.out.println(runningJob.getID());
                } else {
                    if (! calvalusHadoopTool.observeStatus(runningJob)) {
                         System.exit(1);
                    }
                }

            }
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage() + " (use option --help for command line help)");
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            if (commandLine != null && commandLine.hasOption("debug")) {
                e.printStackTrace();
            }
            System.exit(1);
        }
    }

    /** Compose and submit Hadoop job, monitor progress (synchronous) or print ID (asynchronous) */

    public RunningJob exec(String userName, String requestPath, Map<String, String> commandLineParameters, Map<Object, Object> configParameters, boolean overwriteOutput)
            throws IOException, InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, org.jdom2.JDOMException, GeneralSecurityException, ParserConfigurationException, SAXException, XMLEncryptionException {

        String auth = commandLineParameters.getOrDefault("auth", (String) configParameters.getOrDefault("auth", "unix"));
        HadoopJobHook hook = parameterizeTokenGenerator(auth, configParameters, userName);

        JobConf jobConf = requestConverter.createJob(requestPath, commandLineParameters, configParameters, hook);

        if (overwriteOutput || Boolean.parseBoolean(jobConf.get("overwrite", "false"))) {
            hadoopConnection.deleteOutputDir(jobConf);
        }
        RunningJob runningJob = hadoopConnection.submitJob(jobConf);
        LOG.info("Production successfully ordered with ID " + runningJob.getID());

        return runningJob;
    }

    /** Poll request state from Hadoop, kill request in case of interrupt */

    public boolean observeStatus(RunningJob runningJob) throws InterruptedException, IOException {
        Thread shutdownHook = new Thread() {
            @Override
            public void run() {
                try {
                    runningJob.killJob();
                } catch (Exception e) {
                    System.err.println("Failed to shutdown production: " + e.getMessage());
                }
            }
        };
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        try {
            while (true) {
                Thread.sleep(STATUS_POLLING_INTERVAL_MILLIS);
                JobStatus status = hadoopConnection.getJobStatus(runningJob.getID());
                StringBuilder accu = new StringBuilder();
                statusConverter.accumulateJobStatus(runningJob.getID().toString(), status, accu);
                LOG.info(accu.toString());
                if (status == null || status.getRunState() == JobStatus.FAILED || status.getRunState() == JobStatus.KILLED) {
                    return false;
                } else if (status.getRunState() == JobStatus.SUCCEEDED) {
                    return true;
                }
            }
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException _e) {}
        }
    }

    /** Inquire status of a list of jobs */

    public boolean getStatus(String userName, String[] ids, Map<String, String> commandLineParameters, Properties configParameters)
            throws IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException, InterruptedException
    {
        Configuration hadoopParameters = requestConverter.createHadoopConf(commandLineParameters, configParameters);
        hadoopConnection.createJobClient(hadoopParameters);

        StringBuilder accu = new StringBuilder();
        statusConverter.initialiseJobStatus(accu);
        if (ids.length == 1) {
            String id = ids[0];
            JobID jobId = JobID.forName(id);
            JobStatus status = hadoopConnection.getJobStatus(jobId);
            statusConverter.accumulateJobStatus(id, status, accu);
        } else {
            JobStatus[] jobs = hadoopConnection.getAllJobs();
            for (String id : ids) {
                JobStatus status = CalvalusHadoopStatusConverter.findById(id, jobs);
                // ask history server if job is no longer in resource manager
                if (status == null) {
                    status = hadoopConnection.getJobStatus(JobID.forName(id));
                }
                statusConverter.accumulateJobStatus(id, status, accu);
            }
        }
        statusConverter.finaliseJobStatus(accu);
        System.out.print(accu.toString());
        return true;
    }

    private boolean cancel(String userName, String[] ids, Map<String, String> commandLineParameters, Properties configParameters)
            throws IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException, InterruptedException
    {
        Configuration hadoopParameters = requestConverter.createHadoopConf(commandLineParameters, configParameters);
        hadoopConnection.createJobClient(hadoopParameters);

        boolean someJobsFound = false;
        if (ids.length == 1) {
            String id = ids[0];
            JobID jobId = JobID.forName(id);
            RunningJob job = hadoopConnection.getJob(jobId);
            if (job != null) {
                job.killJob();
                someJobsFound = true;
            } else {
                LOG.warning("job " + id + " not found for cancelling");
            }
        } else {
            for (String id : ids) {
                JobID jobId = JobID.forName(id);
                RunningJob job = hadoopConnection.getJob(jobId);
                if (job != null) {
                    job.killJob();
                    someJobsFound = true;
                } else {
                    LOG.warning("job " + id + " not found for cancelling");
                }
            }
        }
        return someJobsFound;
    }

    /** Retrieve SAML token from CAS or generate debug SAML token */

    private HadoopJobHook parameterizeTokenGenerator(String authPolicy, Map<Object,Object> configParameters, String userName)
            throws GeneralSecurityException, ParserConfigurationException, org.jdom2.JDOMException, IOException, SAXException, XMLEncryptionException {
        switch (authPolicy) {
            case "unix":
                return null;
            case "debug":
                LOG.warning("using debug token for processing authentication");
                String publicKey = (String) configParameters.get("calvalus.crypt.calvalus-public-key");
                String privateKey = (String) configParameters.get("calvalus.crypt.debug-private-key");
                String certificate = (String) configParameters.get("calvalus.crypt.debug-certificate");
                return new DebugTokenGenerator(publicKey, privateKey, certificate, userName);
            case "saml":
                LOG.info("requesting SAML token from CAS");
                String samlToken = new CasUtil(LOG.isLoggable(Level.INFO)).fetchSamlToken2(configParameters, userName).replace("\\s+", "");
                String publicKeySaml = (String) configParameters.get("calvalus.crypt.calvalus-public-key");
                return new TokenGenerator(publicKeySaml, samlToken);
            default:
                throw new RuntimeException("unknown auth type " + authPolicy);
        }
    }

    /**
     * Command line parameter declarations
     */
    private static Options createCommandLineOptions() {
        Options options = new Options();
        options.addOption(OptionBuilder
                                  .withLongOpt("quiet")
                                  .withDescription("Quiet mode, minimum console output.")
                                  .create("q"));
        options.addOption(OptionBuilder
                                  .withLongOpt("debug")
                                  .withDescription("Debug mode, print stack trace of exception.")
                                  .create("e"));
        options.addOption(OptionBuilder
                                  .withLongOpt("overwrite")
                                  .withDescription("Delete target directory before submission.")
                                  .create("o"));
        options.addOption(OptionBuilder
                                  .withLongOpt("async")
                                  .withDescription("Asynchronous mode, submission.")
                                  .create("a"));
        options.addOption(OptionBuilder
                                  .withLongOpt("status")
                                  .withDescription("Asynchronous mode, status inquiry.")
                                  .create()); // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("cancel")
                                  .withDescription("Asynchronous mode, cancelling.")
                                  .create()); // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("help")
                                  .withDescription("Prints out usage help.")
                                  .create()); // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("test-auth")
                                  .withDescription("Requests SAML token only.")
                                  .create()); // (sub) commands don't have short options
        options.addOption(OptionBuilder
                                  .withLongOpt("calvalus")
                                  .hasArg()
                                  .withArgName("NAME")
                                  .withDescription(
                                          "The name of the Calvalus software bundle used for the production.")
                                  .create("c"));
        options.addOption(OptionBuilder
                                  .withLongOpt("snap")
                                  .hasArg()
                                  .withArgName("NAME")
                                  .withDescription(
                                          "The name of the SNAP software bundle used for the production.")
                                  .create("s"));
        options.addOption(OptionBuilder
                                  .withLongOpt("config")
                                  .hasArg()
                                  .withArgName("FILE")
                                  .withDescription(
                                          "The Calvalus configuration file (Java properties format).")
                                  .create("p"));
        options.addOption(OptionBuilder
                                  .withLongOpt("auth")
                                  .hasArg()
                                  .withArgName("NAME")
                                  .withDescription(
                                          "Authentication method. One of unix, saml, debug.")
                                  .create("u"));
        options.addOption(OptionBuilder
                                  .withLongOpt("format")
                                  .hasArg()
                                  .withArgName("NAME")
                                  .withDescription(
                                          "Status output format. One of json, csv.")
                                  .create("f"));
        return options;
    }
}
