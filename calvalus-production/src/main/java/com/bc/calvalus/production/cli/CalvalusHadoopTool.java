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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.security.UserGroupInformation;
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

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class CalvalusHadoopTool {

    private static final TypeReference<Map<String, Object>> VALUE_TYPE_REF = new TypeReference<Map<String, Object>>() {};
    private static final SimpleDateFormat ISO_MILLIS_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    public static final String CALVALUS_SOFTWARE_PATH = "/calvalus/software/1.0";

    private boolean quiet;

    public static void main(String[] args) {
        try {
            String userName = "martin"; //System.getProperty("user.name");
            boolean quiet = false;

            Options options = createCommandLineOptions();
            CommandLine commandLine = new GnuParser().parse(options, args);

            if (commandLine.hasOption("help")) {
                new HelpFormatter().printHelp("cht [OPTION]... REQUEST",
                                              "\nThe Calvalus Hadoop Tool translates a request with some production type to Hadoop parameters and submits a job to Hadoop YARN. OPTION may be one or more of the following:",
                                              options,
                                              "", false);
                System.exit(0);
            }

            // collect command line parameters and .calvalus/calvalus.config parameters

            Map<String, String> commandLineParameters = collectCommandLineParameters(commandLine);
            Properties configParameters = collectConfigParameters(commandLine);

            // handle test-auth

            if (commandLine.hasOption("test-auth")) {
                String samlToken = new CasUtil(false).fetchSamlToken2(configParameters, userName);
                System.out.println("Successfully retrieved SAML token:\n");
                System.out.println(samlToken);
            }

            // maybe retrieve SAML token from CAS

            String authPolicy;
            if (commandLine.hasOption("auth")) {
                authPolicy = commandLine.getOptionValue("auth");
            } else {
                authPolicy = (String) configParameters.getOrDefault("calvalus.crypt.auth", "unix");
            }
            HadoopJobHook hook = null;
            switch (authPolicy) {
                case "unix":
                    break;
                case "debug":
                    String publicKey = (String) configParameters.get("calvalus.crypt.calvalus-public-key");
                    String privateKey = (String) configParameters.get("calvalus.crypt.debug-private-key");
                    String certificate = (String) configParameters.get("calvalus.crypt.debug-certificate");
                    hook = new DebugTokenGenerator(publicKey, privateKey, certificate, userName);
                    break;
                case "saml":
                    String samlToken = new CasUtil(quiet).fetchSamlToken2(configParameters, userName).replace("\\s+", "");
                    String publicKeySaml = (String) configParameters.get("calvalus.crypt.calvalus-public-key");
                    hook = new TokenGenerator(publicKeySaml, samlToken);
                    break;
                default:
                    throw new RuntimeException("unknown auth type " + authPolicy);
            }

            // execute request

            if (commandLine.getArgList().size() != 1) {
                System.err.println("Error: One argument REQUEST expected. (use option --help for usage help)");
                System.exit(1);
            }

            String requestPath = String.valueOf(commandLine.getArgList().get(0));

            new CalvalusHadoopTool().exec(requestPath, commandLineParameters, configParameters, userName, hook);
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage() + " (use option --help for command line help)");
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

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

    private static Map<String, String> collectCommandLineParameters(CommandLine commandLine) {
        Map<String,String> commandLineParameters = new HashMap<String,String>();
        for (Option option : commandLine.getOptions()) {
            if (! "config".equals(option.getLongOpt())) {
                commandLineParameters.put(option.getLongOpt(), commandLine.getOptionValue(option.getOpt()));
            }
        }
        return commandLineParameters;
    }

    private Map<String,Object> parseIntoMap(String path) throws IOException {
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
                            parameterValue = getStringFromElement(literalDataElement, xmlOutputter);
                        } else {
                            Element complexDataElement = dataElement.getChild("ComplexData", wps);
                            if (complexDataElement != null) {
                                parameterValue = getStringFromElement(complexDataElement, xmlOutputter);
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

    public void exec(String requestPath, Map<String, String> commandLineParameters, Map<Object, Object> configParameters, String userName, HadoopJobHook hook)
            throws IOException, InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(userName);

        // read request and production type definition
        Map<String, Object> request = parseIntoMap(requestPath);
        String productionTypeName = getParameter(request, "productionType", "calvalus.productionType");
        Map<String, Object> productionTypeDef = parseIntoMap("etc/" + productionTypeName + "-cht-type.json");

        // create Hadoop job config with Hadoop defaults
        final String productionId = Production.createId(productionTypeName);

        final Configuration hadoopParameters = new Configuration(false);

        setHadoopDefaultParameters(hadoopParameters);

        printParameters("Hadoop defaults", hadoopParameters);

        // set parameters by tool
        hadoopParameters.set("calvalus.user", userName);
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
        JobClient jobClient = remoteUser.doAs((PrivilegedExceptionAction<JobClient>) () -> new JobClient(new JobConf(hadoopParameters)));
        FileSystem fileSystem = jobClient.getFs();

        // retrieve and add parameters of processor descriptor
        for (Map.Entry<String,String> entry : getProcessorDescriptorParameters(userName, remoteUser, fileSystem, hadoopParameters).entrySet()) {
            translateAndInsert(entry.getKey(), entry.getValue(), productionTypeDef, hadoopParameters);
        }

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
        ProcessorFactory.installProcessorBundles(userName, jobConf, fileSystem);
        final String calvalusBundle = hadoopParameters.get(JobConfigNames.CALVALUS_CALVALUS_BUNDLE);
        final String snapBundle = hadoopParameters.get(JobConfigNames.CALVALUS_SNAP_BUNDLE);
        if (calvalusBundle != null) {
            installBundle(calvalusBundle, fileSystem, jobConf);
        }
        if (snapBundle != null) {
            installBundle(snapBundle, fileSystem, jobConf);
        }
        if (hook != null) {
            hook.beforeSubmit(jobConf);
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
            JobStatus job = jobClient.getJobStatus(runningJob.getID());
//            JobStatus[] allJobs = jobClient.getAllJobs();
//            JobStatus job = null;
//            for (JobStatus job1 : allJobs) {
//                if (runningJob.getID().equals(job1.getJobID())) {
//                    job = job1;
//                    break;
//                }
//            }
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

    private void setHadoopDefaultParameters(Configuration hadoopParameters) {
        hadoopParameters.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
        hadoopParameters.set("fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs");
        hadoopParameters.set("fs.hdfs.impl.disable.cache", "true");
        hadoopParameters.set("mapred.mapper.new-api", "true");
        hadoopParameters.set("mapred.reducer.new-api", "true");
        hadoopParameters.set("mapreduce.framework.name", "yarn");
        hadoopParameters.set("mapreduce.client.genericoptionsparser.used", "true");
        hadoopParameters.set("yarn.log-aggregation-enable", "true");
        hadoopParameters.set("dfs.client.read.shortcircuit", "true");
        hadoopParameters.set("dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket");

        hadoopParameters.set("dfs.blocksize", "2147483136");
        hadoopParameters.set("io.file.buffer.size", "131072");
        hadoopParameters.set("dfs.replication", "1");
        hadoopParameters.set("mapreduce.map.speculative", "false");
        hadoopParameters.set("mapreduce.reduce.speculative", "false");
        hadoopParameters.set("dfs.permissions.superusergroup", "hadoop");
        hadoopParameters.set("fs.permissions.umask-mode", "002");
        hadoopParameters.set("yarn.app.mapreduce.am.command-opts", "-Xmx512M -Djava.awt.headless=true");
        hadoopParameters.set("yarn.app.mapreduce.am.resource.mb", "512");
    }

    private String getStringFromElement(Element elem, XMLOutputter xmlOutputter) throws IOException {
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

    private Map<String, String> getProcessorDescriptorParameters(String userName, UserGroupInformation remoteUser, FileSystem fileSystem, Configuration hadoopParameters) throws IOException, InterruptedException {
        String bundles = hadoopParameters.get("calvalus.bundles");
        String processor = hadoopParameters.get("calvalus.l2.operator");
        if (bundles == null || processor == null) {
            return Collections.emptyMap();
        }
        return remoteUser.doAs((PrivilegedExceptionAction<Map<String,String>>) () -> {
            Path path = new Path("/calvalus/software/1.0/" + bundles.split(",")[0] + "/bundle-descriptor.xml");
            FileStatus status = fileSystem.getFileStatus(path);
            if (status == null) {
                path = new Path("/calvalus/home/" + userName + "/software/" + bundles.split(",")[0] + "/bundle-descriptor.xml");
                status = fileSystem.getFileStatus(path);
            }
            if (status == null) {
                return Collections.emptyMap();
            }
            BundleDescriptor bd = readBundleDescriptor(fileSystem, path);
            for (ProcessorDescriptor pd : bd.getProcessorDescriptors()) {
                if (processor.equals(pd.getExecutableName())) {
                    return pd.getJobConfiguration();
                }
            }
            return Collections.emptyMap();
        });
    }

    private static BundleDescriptor readBundleDescriptor(FileSystem fileSystem, Path path) throws IOException, BindingException {
        final ParameterBlockConverter parameterBlockConverter = new ParameterBlockConverter();
        final BundleDescriptor bd = new BundleDescriptor();
        parameterBlockConverter.convertXmlToObject(readFile(fileSystem, path), bd);
        return bd;
    }

    private static String readFile(FileSystem fileSystem, Path path) throws IOException {
        try (InputStream is = fileSystem.open(path);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            IOUtils.copyBytes(is, baos, 8092);
            return baos.toString();
        }
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

    private void say(String message) {
        if (!quiet) {
            System.out.println(message);
        }
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
