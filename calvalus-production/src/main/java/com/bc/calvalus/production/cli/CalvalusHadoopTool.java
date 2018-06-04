package com.bc.calvalus.production.cli;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionServiceConfig;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import javafx.beans.binding.ObjectExpression;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class CalvalusHadoopTool {

    private static final TypeReference<Map<String, Object>> VALUE_TYPE_REF = new TypeReference<Map<String, Object>>() {};
    private static final SimpleDateFormat ISO_MILLIS_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    public static final String CALVALUS_SOFTWARE_PATH = "/calvalus/software/1.0";

    private final ObjectMapper mapper;

    public CalvalusHadoopTool() {
        mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    }

    public static void main(String[] args) {
        try {
            Options options = createCommandLineOptions();
            CommandLine commandLine = new GnuParser().parse(options, args);

            if (commandLine.hasOption("help")) {
                new HelpFormatter().printHelp("cht [OPTION]... REQUEST",
                                              "\nThe Calvalus Hadoop Tool translates a request with some production type to Hadoop parameters and submits a job to Hadoop YARN. OPTION may be one or more of the following:",
                                              options,
                                              "", false);
                System.exit(0);
            }

            // collect command line parameters

            Map<String,String> commandLineParameters = new HashMap<String,String>();
            for (Option option : commandLine.getOptions()) {
                if (! "config".equals(option.getLongOpt())) {
                    commandLineParameters.put(option.getLongOpt(), commandLine.getOptionValue(option.getOpt()));
                }
            }

            // read .calvalus/calvalus.config tool config

            Properties configParameters = new Properties();
            File configFile;
            if (commandLine.hasOption("config")) {
                configFile = new File(commandLine.getOptionValue("config"));
            } else {
                configFile = new File(new File(new File(System.getProperty("user.home")), ".calvalus"), "calvalus.config");
            }
            if (configFile.exists()) {
                try (FileReader reader = new FileReader(configFile)) {
                    configParameters.load(reader);
                }
            }

            if (commandLine.getArgList().size() != 1) {
                System.err.println("Error: One argument REQUEST expected. (use option --help for usage help)");
                System.exit(1);
            }
            String requestPath = String.valueOf(commandLine.getArgList().get(0));

            new CalvalusHadoopTool().exec(requestPath, commandLineParameters, configParameters);
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage() + " (use option --help for command line help)");
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void exec(String requestPath, Map<String,String> commandLineParameters, Map<Object,Object> configParameters)
            throws IOException, InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        // read request and production type definition
        Map<String, Object> request = mapper.readValue(new File(requestPath), new TypeReference<Map<String, Object>>() {});
        String productionTypeName = getParameter(request, "productionType", "calvalus.productionType");
        Map<String, Object> productionTypeDef = mapper.readValue(new File("etc/" + productionTypeName + "-cht-type.json"), VALUE_TYPE_REF);

        // create Hadoop job config with Hadoop defaults
        String userName = "martin"; //System.getProperty("user.name");
        final String productionId = Production.createId(productionTypeName);

        final Configuration hadoopParameters = new Configuration();

        printParameters("Hadoop defaults", hadoopParameters);

        // set parameters by tool
        hadoopParameters.set("mapreduce.framework.name", "yarn");
        hadoopParameters.set("dfs.blocksize", "2147483136");
        hadoopParameters.set("io.file.buffer.size", "131072");
        hadoopParameters.set("dfs.replication", "1");
        hadoopParameters.set("dfs.client.read.shortcircuit", "true");
        hadoopParameters.set("dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket");
        hadoopParameters.set("mapreduce.map.speculative", "false");
        hadoopParameters.set("mapreduce.reduce.speculative", "false");
        hadoopParameters.set("mapreduce.client.genericoptionsparser.used", "true");
        hadoopParameters.set("dfs.permissions.superusergroup", "hadoop");
        hadoopParameters.set("fs.permissions.umask-mode", "002");
        hadoopParameters.set("yarn.log-aggregation-enable", "true");
        hadoopParameters.set("yarn.app.mapreduce.am.command-opts", "-Xmx512M -Djava.awt.headless=true");
        hadoopParameters.set("yarn.app.mapreduce.am.resource.mb", "512");

        hadoopParameters.set("calvalus.user", userName);
        hadoopParameters.set("fs.hdfs.impl.disable.cache", "true");
        hadoopParameters.set("mapred.mapper.new-api", "true");
        hadoopParameters.set("mapred.reducer.new-api", "true");
        hadoopParameters.set("calvalus.output.dir", "/calvalus/home/%s/%s".format(userName, productionId));
        //jobConfTemplate.set("jobSubmissionDate", ISO_MILLIS_FORMAT.format(new Date()));

        // add parameters of config, maybe translate and apply function
        for (Map.Entry<Object,Object> entry : configParameters.entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (key.startsWith("calvalus.hadoop.")) {
                key = key.substring("calvalus.hadoop.".length());
            }
            translateAndInsert(key, String.valueOf(entry.getValue()), productionTypeDef, hadoopParameters);
        }

        // add parameters of production type, maybe translate and apply function
        for (Map.Entry<String,Object> entry : productionTypeDef.entrySet()) {
            if (! entry.getKey().startsWith("_")) {
                translateAndInsert(entry.getKey(), String.valueOf(entry.getValue()), productionTypeDef, hadoopParameters);
            }
        }

        // add parameters of request, maybe translate and apply function
        for (Map.Entry<String,Object> entry : request.entrySet()) {
            translateAndInsert(entry.getKey(), String.valueOf(entry.getValue()), productionTypeDef, hadoopParameters);
        }

        // add parameters of command line, maybe translate and apply function
        for (Map.Entry<String,String> entry : commandLineParameters.entrySet()) {
            translateAndInsert(entry.getKey(), entry.getValue(), productionTypeDef, hadoopParameters);
        }

        // create job client for user and access to file system
        UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(userName);
        JobClient jobClient = remoteUser.doAs((PrivilegedExceptionAction<JobClient>) () -> new JobClient(new JobConf(hadoopParameters)));
        JobConf jobConf = new JobConf(new Configuration(jobClient.getConf()));
        FileSystem fileSystem = jobClient.getFs();

        // install processor bundles and calvalus and snap bundle
        ProcessorFactory.installProcessorBundles(userName, jobConf, fileSystem);
        final String calvalusBundle = jobConf.get(JobConfigNames.CALVALUS_CALVALUS_BUNDLE);
        final String snapBundle = jobConf.get(JobConfigNames.CALVALUS_SNAP_BUNDLE);
        if (calvalusBundle != null) {
            installBundle(calvalusBundle, fileSystem, jobConf);
        }
        if (snapBundle != null) {
            installBundle(snapBundle, fileSystem, jobConf);
        }

        printParameters("Parameterised job", jobConf);

        RunningJob runningJob = jobClient.submitJob(jobConf);
        System.out.println("Production successfully ordered. The production ID is: " + runningJob.getID());

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

        while (true) {
            Thread.sleep(5000);
            JobStatus[] allJobs = jobClient.getAllJobs();
            JobStatus job = null;
            for (JobStatus job1 : allJobs) {
                if (runningJob.getID().equals(job1.getJobID())) {
                    job = job1;
                    break;
                }
            }
            if (job == null) {
                System.err.println("Job " + runningJob.getID() + " not found in YARN");
                System.exit(1);
            }
            System.out.println("state " + job.getState() + " map " + job.getMapProgress() + " reduce " + job.getReduceProgress());
            if (job.getRunState() == JobStatus.FAILED || job.getRunState() == JobStatus.KILLED) {
                int eventCounter = 0;
                while (true) {
                    TaskCompletionEvent[] taskCompletionEvents = runningJob.getTaskCompletionEvents(eventCounter);
                    if (taskCompletionEvents.length == 0) {
                        break;
                    }
                    eventCounter += taskCompletionEvents.length;
                    for (TaskCompletionEvent taskCompletionEvent : taskCompletionEvents) {
                        if (taskCompletionEvent.getTaskStatus().equals(TaskCompletionEvent.Status.FAILED)) {
                            String[] taskDiagnostics = runningJob.getTaskDiagnostics(taskCompletionEvent.getTaskAttemptId());
                            if (taskDiagnostics.length > 0) {
                                String firstMessage = taskDiagnostics[0];
                                String firstLine = firstMessage.split("\\n")[0];
                                String[] firstLineSplit = firstLine.split("Exception: ");
                                System.err.println(firstLineSplit[firstLineSplit.length - 1]);
                                break;
                            }
                        }
                    }
                }
                System.exit(1);
            } else if (job.getRunState() == JobStatus.SUCCEEDED) {
               break;
            }
        }
        Runtime.getRuntime().removeShutdownHook(shutdownHook);

        //if (production.getProcessingStatus().getState() == ProcessState.COMPLETED) {
        //    say("Production completed. Output directory is " + production.getStagingPath());
        //} else {
        //    exit("Error: Production did not complete normally: " + production.getProcessingStatus().getMessage(), 2);
        //}
    }

    private void installBundle(String calvalusBundle, FileSystem fileSystem, JobConf jobConf) throws IOException {
        Path bundlePath = new Path(CALVALUS_SOFTWARE_PATH, calvalusBundle);
        HadoopProcessingService.addBundleToClassPathStatic(bundlePath, jobConf, fileSystem);
        HadoopProcessingService.addBundleArchives(bundlePath, fileSystem, jobConf);
        HadoopProcessingService.addBundleLibs(bundlePath, fileSystem, jobConf);
    }

    private void translateAndInsert(String key, String value, Map<String, Object> productionTypeDef, Configuration jobConf)
            throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        String translationKey = "_translate." + key;
        Object translations = productionTypeDef.get(translationKey);
        if (translations == null) {
            jobConf.set(key, value);
        } else if (translations instanceof String) {
            jobConf.set((String) translations, value);
        } else {
            for (Object translation : (ArrayList<Object>) translations) {
                if (translation instanceof String) {
                    jobConf.set((String) translation, value);
                } else {
                    String hadoopKey = String.valueOf(((ArrayList<Object>) translation).get(0));
                    String functionName = String.valueOf(((ArrayList<Object>) translation).get(1));
                    String hadoopValue = (String) getClass().getMethod(functionName, String.class).invoke(this, value);
                    jobConf.set(hadoopKey, hadoopValue);
                }
            }
        }
    }

    private void printParameters(String header, Configuration jobConf) {
        System.out.println(header);
        System.out.println(jobConf);
        Iterator<Map.Entry<String, String>> iterator = jobConf.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        System.out.println();
    }

    private static String getParameter(Map<String, Object> request, String... names) {
        for (String name : names) {
            if (name != null) {
                Object value = request.get(name);
                if (value != null) {
                    return String.valueOf(value);
                }
            }
        }
        throw new IllegalArgumentException("missing parameter " + names[0]);
    }

    private static Options createCommandLineOptions() {
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
                        "The name of the Calvalus software bundle used for the production.")
                .create("C"));
        options.addOption(OptionBuilder
                .withLongOpt("snap")
                .hasArg()
                .withArgName("NAME")
                .withDescription(
                        "The name of the SNAP software bundle used for the production.")
                .create("S"));
        options.addOption(OptionBuilder
                .withLongOpt("config")
                .hasArg()
                .withArgName("FILE")
                .withDescription(
                        "The Calvalus configuration file (Java properties format).")
                .create("c"));
        options.addOption(OptionBuilder
                .withLongOpt("auth")
                .hasArg()
                .withArgName("NAME")
                .withDescription(
                        "Authentication method. One of unix, saml, debug.")
                .create("a"));
        return options;
    }

    public String seconds2Millis(String seconds) {
        return seconds + "000";
    }
    public String javaOptsOfMem(String mem) {
        return "-Djava.awt.headless=true -Xmx" + mem + "M";
    }
    public String add512(String mem) {
        return String.valueOf(Integer.parseInt(mem)+512);
    }
}
