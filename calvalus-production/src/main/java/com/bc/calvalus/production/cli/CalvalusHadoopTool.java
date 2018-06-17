package com.bc.calvalus.production.cli;

import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.HadoopJobHook;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.util.CasUtil;
import com.bc.calvalus.production.util.DebugTokenGenerator;
import com.bc.calvalus.production.util.TokenGenerator;
import com.bc.ceres.binding.BindingException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
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
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.xml.security.encryption.XMLEncryptionException;
import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.esa.snap.core.util.io.FileUtils;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class CalvalusHadoopTool {

    private static final TypeReference<Map<String, Object>> VALUE_TYPE_REF = new TypeReference<Map<String, Object>>() {};
    private static final SimpleDateFormat ISO_MILLIS_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private static final String CALVALUS_SOFTWARE_PATH = "/calvalus/software/1.0";
    private static final int STATUS_POLLING_INTERVAL_MILLIS = 5000;

    private boolean quiet;
    private boolean asynchronous;
    private String userName;
    private UserGroupInformation remoteUser;
    private Configuration hadoopParameters;
    private JobClient jobClient;
    private RunningJob runningJob;

    public CalvalusHadoopTool(boolean quiet, boolean asynchronous, String userName) {
        this.quiet = quiet;
        this.asynchronous = asynchronous;
        this.userName = userName;
        remoteUser = UserGroupInformation.createRemoteUser(userName);
    }

    public static void main(String[] args) {
        try {
            Options options = createCommandLineOptions();
            CommandLine commandLine = new GnuParser().parse(options, args);
            Properties configParameters = collectConfigParameters(commandLine);
            String userName = "martin"; // TODO System.getProperty("user.name");
            boolean quiet = commandLine.hasOption("quiet");
            boolean asynchronous = commandLine.hasOption("async");

            if (commandLine.hasOption("help")) {
                new HelpFormatter().printHelp("cht [OPTION]... REQUEST|IDs",
                                              "\nThe Calvalus Hadoop Tool translates a request into a Hadoop job, submits it, inquires status. OPTION may be one or more of the following:",
                                              options,
                                              "", false);
            } else if (commandLine.hasOption("test-auth")) {
                String samlToken = new CasUtil(false).fetchSamlToken2(configParameters, userName);
                System.out.println("Successfully retrieved SAML token:\n");
                System.out.println(samlToken);
            } else if (commandLine.hasOption("status") && commandLine.getArgList().size() < 1) {
                System.err.println("Error: At least one argument JOBID expected. (use option --help for usage help)");
                System.exit(1);
            } else if (commandLine.hasOption("status")) {
                String[] ids = commandLine.getArgs();
                Map<String, String> commandLineParameters = collectCommandLineParameters(commandLine);
                if (! new CalvalusHadoopTool(true, true, userName).getStatus(ids, commandLineParameters, configParameters)) {
                    System.exit(1);
                }
            } else if (! commandLine.hasOption("status") && commandLine.getArgList().size() != 1) {
                System.err.println("Error: One argument REQUEST expected. (use option --help for usage help)");
                System.exit(1);
            } else {
                String requestPath = String.valueOf(commandLine.getArgList().get(0));
                Map<String, String> commandLineParameters = collectCommandLineParameters(commandLine);
                if (! new CalvalusHadoopTool(quiet, asynchronous, userName).exec(requestPath, commandLineParameters, configParameters)) {
                    System.exit(1);
                }
            }
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage() + " (use option --help for command line help)");
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /** Inquire status of a list of jobs */

    private boolean getStatus(String[] ids, Map<String, String> commandLineParameters, Properties configParameters)
            throws IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException, InterruptedException {
        // create Hadoop job config with Hadoop defaults
        hadoopParameters = new Configuration(false);
        setHadoopDefaultParameters(hadoopParameters);

        // set parameters by tool
        hadoopParameters.set("calvalus.user", userName);

        // add parameters of config, maybe translate and apply function
        for (Map.Entry<Object, Object> entry : configParameters.entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (key.startsWith("calvalus.hadoop.")) {
                key = key.substring("calvalus.hadoop.".length());
            }
            translateAndInsert(key, String.valueOf(entry.getValue()), null, hadoopParameters);
        }
        for (Map.Entry<String, String> entry : commandLineParameters.entrySet()) {
            translateAndInsert(entry.getKey(), entry.getValue(), null, hadoopParameters);
        }

        // create job client for user for status inquiry and for access to file system
        jobClient = remoteUser.doAs((PrivilegedExceptionAction<JobClient>) () -> new JobClient(new JobConf(hadoopParameters)));

        // inquire single status or all job stati
        StringBuilder accu = new StringBuilder("{");
        if (ids.length == 1) {
            String id = ids[0];
            JobID jobId = JobID.forName(id);
            JobStatus status = jobClient.getJobStatus(jobId);
            accumulateJobStatus(id, status, accu);
        } else {
            JobStatus[] jobs = jobClient.getAllJobs();
            for (String id : ids) {
                JobStatus status = findById(id, jobs);
                accumulateJobStatus(id, status, accu);
            }
        }
        accu.append("}");
        System.out.println(accu.toString());
        return true;
    }

    /** Look up job status by ID */

    private static JobStatus findById(String id, JobStatus[] jobs) {
        for (JobStatus status : jobs) {
            if (status.getJobID().toString().equals(id)) {
                return status;
            }
        }
        return null;
    }

    /** Convert job status into Json string */

    private void accumulateJobStatus(String id, JobStatus job, StringBuilder accu) throws IOException {
        if (accu.length() > 1) {
            accu.append(", ");
        }
        double progress = job.getMapProgress() * 0.9 + job.getReduceProgress() * 0.1;
        if (job == null) {
            accumulateJobStatus(id, "NOT_FOUND", progress, null, accu);
        } else if (job.getRunState() == JobStatus.SUCCEEDED) {
            accumulateJobStatus(id, "SUCCEEDED", progress, null, accu);
        } else if (job.getRunState() == JobStatus.FAILED) {
            accumulateJobStatus(id, "FAILED", progress, getDiagnostics(jobClient.getJob(job.getJobID())), accu);
        } else if (job.getRunState() == JobStatus.KILLED) {
            accumulateJobStatus(id, "CANCELLED", progress, getDiagnostics(jobClient.getJob(job.getJobID())), accu);
        } else if (job.getRunState() == JobStatus.RUNNING) {
            accumulateJobStatus(id, "RUNNING", progress, null, accu);
        } else if (job.getRunState() == JobStatus.PREP) {
            accumulateJobStatus(id, "WAITING", progress, null, accu);
        } else {
            throw new IllegalArgumentException("unknown status " + job.getRunState());
        }
    }

    /** Compose Json string for status */
    
    private void accumulateJobStatus(String id, String status, double progress, String message, StringBuilder accu) {
        accu.append("\"");
        accu.append(id);
        accu.append("\": { \"status\": \"");
        accu.append(status);
        accu.append("\", \"progress\": ");
        accu.append(String.format("%d5.3", progress));
        if (message != null) {
            accu.append(", \"message\": \"");
            accu.append(message);
            accu.append("\"");
        }
        accu.append("}");
    }

    /** Compose and submit Hadoop job, monitor progress (synchronous) or print ID (asynchronous) */

    public boolean exec(String requestPath, Map<String, String> commandLineParameters, Map<Object,Object> configParameters)
            throws IOException, InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, org.jdom2.JDOMException, GeneralSecurityException, ParserConfigurationException, SAXException, XMLEncryptionException {

        // maybe retrieve SAML token from CAS

        HadoopJobHook hook = parameterizeTokenGenerator(
                commandLineParameters.getOrDefault("auth", (String) configParameters.getOrDefault("auth","unix")),
                configParameters,
                userName);

        // create and parameterise Hadoop job

        JobConf jobConf = createJob(requestPath, commandLineParameters, configParameters, hook);

        if (! quiet) {
            printParameters("Parameterised job", jobConf);
        }

        // submit job

        runningJob = jobClient.submitJob(jobConf);

        say("Production successfully ordered with ID " + runningJob.getID());

        if (asynchronous) {
            System.out.println(runningJob.getID());
            return true;
        } else {
            return observeState();
        }
    }

    /** Retrieve SAML token from CAS or generate debug SAML token */

    private HadoopJobHook parameterizeTokenGenerator(String authPolicy, Map<Object,Object> configParameters, String userName)
            throws GeneralSecurityException, ParserConfigurationException, org.jdom2.JDOMException, IOException, SAXException, XMLEncryptionException {
        switch (authPolicy) {
            case "unix":
                return null;
            case "debug":
                say("using debug token for processing authentication");
                String publicKey = (String) configParameters.get("calvalus.crypt.calvalus-public-key");
                String privateKey = (String) configParameters.get("calvalus.crypt.debug-private-key");
                String certificate = (String) configParameters.get("calvalus.crypt.debug-certificate");
                return new DebugTokenGenerator(publicKey, privateKey, certificate, userName);
            case "saml":
                say("requesting SAML token from CAS");
                String samlToken = new CasUtil(quiet).fetchSamlToken2(configParameters, userName).replace("\\s+", "");
                String publicKeySaml = (String) configParameters.get("calvalus.crypt.calvalus-public-key");
                return new TokenGenerator(publicKeySaml, samlToken);
            default:
                throw new RuntimeException("unknown auth type " + authPolicy);
        }
    }

    /** Parameterise Hadoop job from command line, configuration, request, bundle descriptor, production type */

    private JobConf createJob(String requestPath, Map<String, String> commandLineParameters, Map<Object, Object> configParameters, HadoopJobHook hook)
            throws IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, InterruptedException {
        // read request and production type definition
        say("reading request from " + requestPath);
        Map<String, Object> request = parseIntoMap(requestPath);
        String productionTypeName = getParameter(request, "productionType", "calvalus.productionType");
        say("reading production type definition from " + "etc/" + productionTypeName + "-cht-type.json");
        Map<String, Object> productionTypeDef = parseIntoMap("etc/" + productionTypeName + "-cht-type.json");

        // create Hadoop job config with Hadoop defaults
        hadoopParameters = new Configuration(false);
        setHadoopDefaultParameters(hadoopParameters);
        say(hadoopParameters.size() + " Hadoop default parameters");

        // set parameters by tool
        final String productionId = Production.createId(productionTypeName);
        hadoopParameters.set("calvalus.output.dir", String.format(userName, productionId));
        hadoopParameters.set("calvalus.user", userName);
        //jobConfTemplate.set("jobSubmissionDate", ISO_MILLIS_FORMAT.format(new Date()));
        say(hadoopParameters.size() + " parameters with tool-provided ones");

        // add parameters of config, maybe translate and apply function
        for (Map.Entry<Object,Object> entry : configParameters.entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (key.startsWith("calvalus.hadoop.")) {
                key = key.substring("calvalus.hadoop.".length());
            }
            translateAndInsert(key, String.valueOf(entry.getValue()), productionTypeDef, hadoopParameters);
        }
        say(hadoopParameters.size() + " parameters with .calvalus config ones");

        // add parameters of production type, maybe translate and apply function
        for (Map.Entry<String,Object> entry : productionTypeDef.entrySet()) {
            if (! entry.getKey().startsWith("_")) {
                translateAndInsert(entry.getKey(), String.valueOf(entry.getValue()), productionTypeDef, hadoopParameters);
            }
        }
        say(hadoopParameters.size() + " parameters with production type ones");

        // add parameters of request, maybe translate and apply function
        for (Map.Entry<String,Object> entry : request.entrySet()) {
            translateAndInsert(entry.getKey(), String.valueOf(entry.getValue()), productionTypeDef, hadoopParameters);
        }
        say(hadoopParameters.size() + " parameters with request ones");

        // add parameters of command line, maybe translate and apply function
        for (Map.Entry<String,String> entry : commandLineParameters.entrySet()) {
            translateAndInsert(entry.getKey(), entry.getValue(), productionTypeDef, hadoopParameters);
        }
        say(hadoopParameters.size() + " parameters with command line ones");

        // create job client for user and access to file system
        jobClient = remoteUser.doAs((PrivilegedExceptionAction<JobClient>) () -> new JobClient(new JobConf(hadoopParameters)));

        // retrieve and add parameters of processor descriptor
        for (Map.Entry<String,String> entry : getProcessorDescriptorParameters(hadoopParameters).entrySet()) {
            translateAndInsert(entry.getKey(), entry.getValue(), productionTypeDef, hadoopParameters);
        }
        say(hadoopParameters.size() + " parameters with command line ones");

        // overwrite with parameters of request, maybe translate and apply function
        for (Map.Entry<String,Object> entry : request.entrySet()) {
            translateAndInsert(entry.getKey(), String.valueOf(entry.getValue()), productionTypeDef, hadoopParameters);
        }

        // overwrite with parameters of command line, maybe translate and apply function
        for (Map.Entry<String,String> entry : commandLineParameters.entrySet()) {
            translateAndInsert(entry.getKey(), entry.getValue(), productionTypeDef, hadoopParameters);
        }

        // install processor bundles and calvalus and snap bundle
        JobConf jobConf = new JobConf(hadoopParameters);
        ProcessorFactory.installProcessorBundles(userName, jobConf, jobClient.getFs());
        final String calvalusBundle = hadoopParameters.get(JobConfigNames.CALVALUS_CALVALUS_BUNDLE);
        if (calvalusBundle != null) {
            installBundle(calvalusBundle, jobConf, jobClient.getFs());
        }
        final String snapBundle = hadoopParameters.get(JobConfigNames.CALVALUS_SNAP_BUNDLE);
        if (snapBundle != null) {
            installBundle(snapBundle, jobConf, jobClient.getFs());
        }
        if (hook != null) {
            hook.beforeSubmit(jobConf);
        }
        return jobConf;
    }

    /** Poll request state from Hadoop, kill request in case of interrupt */

    private boolean observeState() throws InterruptedException, IOException {
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
                JobStatus job = jobClient.getJobStatus(runningJob.getID());
                if (job == null) {
                    System.err.println("Job " + runningJob.getID() + " not found in YARN");
                    return false;
                }
                System.out.println("state " + job.getState() + " map " + job.getMapProgress() + " reduce " + job.getReduceProgress());
                if (job.getRunState() == JobStatus.FAILED || job.getRunState() == JobStatus.KILLED) {
                    String diagnostics = getDiagnostics(runningJob);
                    System.err.println(diagnostics);
                    return false;
                } else if (job.getRunState() == JobStatus.SUCCEEDED) {
                    return true;
                }
            }
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException _e) {}
        }
    }

    /** Read first exception message of some failed task */

    private static String getDiagnostics(RunningJob runningJob) throws IOException {
        int eventCounter = 0;
        while (true) {
            TaskCompletionEvent[] taskCompletionEvents = runningJob.getTaskCompletionEvents(eventCounter);
            if (taskCompletionEvents.length == 0) {
                return null;
            }
            eventCounter += taskCompletionEvents.length;
            for (TaskCompletionEvent taskCompletionEvent : taskCompletionEvents) {
                if (taskCompletionEvent.getTaskStatus().equals(TaskCompletionEvent.Status.FAILED)) {
                    String[] taskDiagnostics = runningJob.getTaskDiagnostics(taskCompletionEvent.getTaskAttemptId());
                    if (taskDiagnostics.length > 0) {
                        String firstMessage = taskDiagnostics[0];
                        String firstLine = firstMessage.split("\\n")[0];
                        String[] firstLineSplit = firstLine.split("Exception: ");
                        return firstLineSplit[firstLineSplit.length - 1];
                    }
                }
            }
        }
    }

    /** Read Calvalus configuration file */

    private static Properties collectConfigParameters(CommandLine commandLine) throws IOException {
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
        return configParameters;
    }

    /** Convert command line into parameters */

    private static Map<String, String> collectCommandLineParameters(CommandLine commandLine) {
        Map<String,String> commandLineParameters = new HashMap<String,String>();
        for (Option option : commandLine.getOptions()) {
            if (! "config".equals(option.getLongOpt())
                    && ! "quiet".equals(option.getLongOpt())
                    && ! "async".equals(option.getLongOpt())
                    && ! "auth".equals(option.getLongOpt())) {
                commandLineParameters.put(option.getLongOpt(), commandLine.getOptionValue(option.getOpt()));
            }
        }
        return commandLineParameters;
    }

    /** Read request or production type definition */

    private static Map<String,Object> parseIntoMap(String path) throws IOException {
        switch (FileUtils.getExtension(path)) {
            case ".json":
                ObjectMapper jsonParser = new ObjectMapper();
                jsonParser.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
                return jsonParser.readValue(new File(path), VALUE_TYPE_REF);
            case ".yaml":
                ObjectMapper yamlParser = new ObjectMapper(new YAMLFactory());
                yamlParser.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
                return yamlParser.readValue(new File(path), VALUE_TYPE_REF);
            case ".xml":
                HashMap<String,Object> parameterMap = new HashMap<String,Object>();
                try (Reader reader = new BufferedReader(new FileReader(new File(path)))) {
                    SAXBuilder saxBuilder = new SAXBuilder();
                    Document document = saxBuilder.build(reader);
                    Format format = Format.getRawFormat().setLineSeparator("\n");
                    XMLOutputter xmlOutputter = new XMLOutputter(format);
                    Element executeElement = document.getRootElement();
                    Namespace wps = executeElement.getNamespace("wps");
                    Namespace ows = executeElement.getNamespace("ows");
                    Namespace xlink = executeElement.getNamespace("xlink");
                    parameterMap.put("productionType", executeElement.getChildText("Identifier", ows));
                    Element dataInputs = executeElement.getChild("DataInputs", wps);
                    List<Element> inputElements = (List<Element>) dataInputs.getChildren("Input", wps);
                    for (Element inputElement : inputElements) {
                        String parameterName = inputElement.getChildText("Identifier", ows).trim();
                        String parameterValue = null;
                        Element dataElement = inputElement.getChild("Data", wps);
                        Element literalDataElement = dataElement.getChild("LiteralData", wps);
                        if (literalDataElement != null) {
                            parameterValue = getElementContent(literalDataElement, xmlOutputter);
                        } else {
                            Element complexDataElement = dataElement.getChild("ComplexData", wps);
                            if (complexDataElement != null) {
                                parameterValue = getElementContent(complexDataElement, xmlOutputter);
                            } else {
                                Element referenceElement = dataElement.getChild("Reference", wps);
                                if (referenceElement != null) {
                                    parameterValue = referenceElement.getAttributeValue("href", xlink);
                                }
                            }
                        }
                        if (parameterValue != null) {
                            parameterValue = parameterValue.trim();
                            if (parameterMap.containsKey(parameterName)) {
                                parameterValue = String.format("%s,%s", parameterMap.get(parameterName), parameterValue);
                            }
                            parameterMap.put(parameterName, parameterValue);
                        }
                    }
                } catch (JDOMException e) {
                    throw new IOException(e);
                }
                return parameterMap;
            default:
                throw new IllegalArgumentException(FileUtils.getExtension(path) + " not supported for requests");
        }
    }

    /** read one XML literal or complex content value */

    private static String getElementContent(Element elem, XMLOutputter xmlOutputter) throws IOException {
        List children = elem.getChildren();
        if (children.size() > 0) {
            StringWriter out = new StringWriter();
            Element complexContent = (Element) children.get(0);
            xmlOutputter.output(complexContent, out);
            return out.toString();
        } else {
            return  elem.getText();
        }
    }

    /** set Calvalus default parameters for Hadoop */

    private static void setHadoopDefaultParameters(Configuration hadoopParameters) {
        hadoopParameters.set("dfs.client.read.shortcircuit", "true");
        hadoopParameters.set("dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket");
        hadoopParameters.set("dfs.blocksize", "2147483136");
        hadoopParameters.set("dfs.replication", "1");
        hadoopParameters.set("dfs.permissions.superusergroup", "hadoop");
        hadoopParameters.set("fs.permissions.umask-mode", "002");
        hadoopParameters.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
        hadoopParameters.set("fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs");
        hadoopParameters.set("fs.hdfs.impl.disable.cache", "true");
        hadoopParameters.set("io.file.buffer.size", "131072");
        hadoopParameters.set("mapred.mapper.new-api", "true");
        hadoopParameters.set("mapred.reducer.new-api", "true");
        hadoopParameters.set("mapreduce.framework.name", "yarn");
        hadoopParameters.set("mapreduce.client.genericoptionsparser.used", "true");
        hadoopParameters.set("mapreduce.map.speculative", "false");
        hadoopParameters.set("mapreduce.reduce.speculative", "false");
        hadoopParameters.set("rpc.engine.org.apache.hadoop.ipc.ProtocolMetaInfoPB", "org.apache.hadoop.ipc.ProtobufRpcEngine");
        hadoopParameters.set("rpc.engine.org.apache.hadoop.mapreduce.v2.api.MRClientProtocolPB", "org.apache.hadoop.ipc.ProtobufRpcEngine");
        hadoopParameters.set("yarn.log-aggregation-enable", "true");
        hadoopParameters.set("yarn.app.mapreduce.am.command-opts", "-Xmx512M -Djava.awt.headless=true");
        hadoopParameters.set("yarn.app.mapreduce.am.resource.mb", "512");
        hadoopParameters.set("yarn.dispatcher.exit-on-error", "true");
        //hadoopParameters.set("calvalus.logs.cpt.maxRetries", "5");
        //hadoopParameters.set("calvalus.logs.cpt.retryPeriodMillis", "1000");
        //hadoopParameters.set("calvalus.logs.maxSizeKb", "100");
    }

    /** read bundle descriptor job parameters */

    private Map<String, String> getProcessorDescriptorParameters(Configuration hadoopParameters)
            throws IOException, InterruptedException {
        String bundles = hadoopParameters.get("calvalus.bundles");
        String processor = hadoopParameters.get("calvalus.l2.operator");
        if (bundles == null || processor == null) {
            say("no bundle or no processor requested");
            return Collections.emptyMap();
        }
        return remoteUser.doAs((PrivilegedExceptionAction<Map<String,String>>) () -> {
            Path path = new Path("/calvalus/software/1.0/" + bundles.split(",")[0] + "/bundle-descriptor.xml");
            if (! jobClient.getFs().exists(path)) {
                path = new Path("/calvalus/home/" + userName + "/software/" + bundles.split(",")[0] + "/bundle-descriptor.xml");
                if (! jobClient.getFs().exists(path)) {
                    say("no bundle-descriptor.xml in bundle " + bundles.split(",")[0]);
                    return Collections.emptyMap();
                }
            }
            BundleDescriptor bd = readBundleDescriptor(path, jobClient.getFs());
            for (ProcessorDescriptor pd : bd.getProcessorDescriptors()) {
                if (processor.equals(pd.getExecutableName())) {
                    say("adding bundle descriptor default parameters from " + path.getName());
                    return pd.getJobConfiguration();
                }
            }
            say("no processor descriptor for " + processor + " in bundle " + bundles.split(",")[0]);
            return Collections.emptyMap();
        });
    }

    /** Read bundle descriptor */

    private static BundleDescriptor readBundleDescriptor(Path path, FileSystem fs) throws IOException, BindingException {
        final ParameterBlockConverter parameterBlockConverter = new ParameterBlockConverter();
        final BundleDescriptor bd = new BundleDescriptor();
        parameterBlockConverter.convertXmlToObject(readFile(fs, path), bd);
        return bd;
    }

    /** Read file content into string */

    private static String readFile(FileSystem fileSystem, Path path) throws IOException {
        try (InputStream is = fileSystem.open(path);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            IOUtils.copyBytes(is, baos, 8096);
            return baos.toString();
        }
    }

    /** Add parameters for classpath and dist cache to Hadoop job */

    private static void installBundle(String calvalusBundle, JobConf jobConf, FileSystem fs) throws IOException {
        Path bundlePath = new Path(CALVALUS_SOFTWARE_PATH, calvalusBundle);
        HadoopProcessingService.addBundleToClassPathStatic(bundlePath, jobConf, fs);
        HadoopProcessingService.addBundleArchives(bundlePath, fs, jobConf);
        HadoopProcessingService.addBundleLibs(bundlePath, fs, jobConf);
    }

    /** Convert Calvalus parameter key and value to Hadoop parameter key and value according to production type definition */

    private void translateAndInsert(String key, String value, Map<String, Object> productionTypeDef, Configuration jobConf)
            throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        String translationKey = "_translate." + key;
        Object translations = productionTypeDef != null ? productionTypeDef.get(translationKey) : null;
        if (translations == null) {
            jobConf.set(key, value);
        } else if (translations instanceof String) {
            say("translating " + key + " to " + translations);
            jobConf.set((String) translations, value);
        } else {
            for (Object translation : (ArrayList<Object>) translations) {
                if (translation instanceof String) {
                    say("translating " + key + " to " + translation);
                    jobConf.set((String) translation, value);
                } else {
                    String hadoopKey = String.valueOf(((ArrayList<Object>) translation).get(0));
                    String functionName = String.valueOf(((ArrayList<Object>) translation).get(1));
                    String hadoopValue = (String) getClass().getMethod(functionName, String.class).invoke(this, value);
                    say("translating " + key + ":" + value + " to " + hadoopKey + ":" + hadoopValue);
                    jobConf.set(hadoopKey, hadoopValue);
                }
            }
        }
    }

    /** Look up parameter using alternative names */

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

    /** List all Hadoop job parameters on console */

    private static void printParameters(String header, Configuration jobConf) {
        System.out.println(header);
        System.out.println(jobConf);
        Iterator<Map.Entry<String, String>> iterator = jobConf.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        System.out.println();
    }

    /** Log message */

    private void say(String message) {
        if (!quiet) {
            System.out.println(message);
        }
    }

    /** Command line parameter declarations */
    private static Options createCommandLineOptions() {
        Options options = new Options();
        options.addOption(OptionBuilder
                .withLongOpt("quiet")
                .withDescription("Quiet mode, minimum console output.")
                .create("q"));
        options.addOption(OptionBuilder
                .withLongOpt("async")
                .withDescription("Asynchronous mode, submission only.")
                .create("a"));
        options.addOption(OptionBuilder
                .withLongOpt("status")
                .withDescription("Asynchronous mode, status inquiry only.")
                .create("s"));
        options.addOption(OptionBuilder
                .withLongOpt("help")
                .withDescription("Prints out usage help.")
                .create("h")); // (sub) commands don't have short options
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
                .create("u"));
        return options;
    }

    /** Function for use in production type translation rules */
    public String seconds2Millis(String seconds) {
        return seconds + "000";
    }
    /** Function for use in production type translation rules */
    public String javaOptsOfMem(String mem) { return "-Djava.awt.headless=true -Xmx" + mem + "M"; }
    /** Function for use in production type translation rules */
    public String javaOptsForExec(String mem) { return "-Djava.awt.headless=true -Xmx384M"; }
    /** Function for use in production type translation rules */
    public String add512(String mem) { return String.valueOf(Integer.parseInt(mem)+512); }
    /** Function for use in production type translation rules */
    public String minDateOf(String dateRanges) { return dateRanges.substring(1,dateRanges.length()-1).split(":")[0].trim(); }
    /** Function for use in production type translation rules */
    public String maxDateOf(String dateRanges) { return dateRanges.substring(1,dateRanges.length()-1).split(":")[1].trim(); }
    /** Function for use in production type translation rules */
    public String minMaxDateRange(String minDate) {
        String dateRanges = hadoopParameters.get("calvalus.input.dateRanges");
        if (dateRanges != null) {
            return String.format(minDate, dateRanges.split(":")[1].trim());
        } else {
            return String.format(minDate);
        }
    }
    /** Function for use in production type translation rules */
    public String maxMinDateRange(String maxDate) {
        String dateRanges = hadoopParameters.get("calvalus.input.dateRanges");
        if (dateRanges != null) {
            return String.format(dateRanges.split(":")[0].trim(), maxDate);
        } else {
            return String.format(maxDate);
        }
    }
    /** Function for use in production type translation rules */
    public String listDateRange(String date) { return String.format(date, date);  }
}
