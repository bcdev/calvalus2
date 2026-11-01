package com.bc.calvalus.production.cli;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopJobHook;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.util.DescriptorUtils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.esa.snap.core.util.io.FileUtils;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CalvalusHadoopRequestConverter {

    private static final SimpleDateFormat ISO_MILLIS_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private static final HashMap<String, Object> EMPTY_FEATURES = new HashMap<>();

    static {
        ISO_MILLIS_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private static final TypeReference<Map<String, Object>> VALUE_TYPE_REF = new TypeReference<Map<String, Object>>() {
    };
    private static Logger LOG = CalvalusLogger.getLogger();

    private final CalvalusHadoopConnection hadoopConnection;
    private final String userName;
    private DescriptorUtils.RoleMatcher roleMatcher = null;
    private final String productionTypeDir;
    private final String processorDescriptorDir;

    public CalvalusHadoopRequestConverter(CalvalusHadoopConnection hadoopConnection, String userName, DescriptorUtils.RoleMatcher roleMatcher, String productionTypeDir, String processorDescriptorDir) {
        this(hadoopConnection, userName, productionTypeDir, processorDescriptorDir);
        this.roleMatcher = roleMatcher;
    }
    public CalvalusHadoopRequestConverter(CalvalusHadoopConnection hadoopConnection, String userName, String productionTypeDir, String processorDescriptorDir) {
        this.hadoopConnection = hadoopConnection;
        this.userName = userName;
        this.productionTypeDir = productionTypeDir;
        this.processorDescriptorDir = processorDescriptorDir;
    }
    public CalvalusHadoopRequestConverter(CalvalusHadoopConnection hadoopConnection, String userName) {
        this(hadoopConnection, userName, "etc", null);
    }


    /**
     * Parses Json string and returns cascaedd map with String leaves
     * @param requestString  Json string
     * @return  map with values that are either string or map
     * @throws IllegalArgumentException raised if request is not valid Json, message contains position
     */
    public Map<String, Object> parseRequest(String requestString) throws IllegalArgumentException {
        try {
            final ObjectMapper jsonParser = new ObjectMapper();
            jsonParser.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
            return jsonParser.readValue(requestString, VALUE_TYPE_REF);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /**
     * Parameterise Hadoop job from command line, configuration, request, bundle descriptor, production type
     */

    /**
     * Merges parameters of command line, request, processor descriptor, Calvalus configuration, production type.
     * Different use cases:
     * - cht tool with command line, request, config
     * - rest submit with request and config
     * - rest list, show, cancel with config, to create hadoop job client
     * Different request versions
     * - with processor (new) or processorName and processorBundles (old), optional
     * - with production type in processor descriptor (new) or in request (old)
     * - with translation rules in calvalus-cht-type.json (new) or in production type (old)
     * Reading sequence:
     * - read processor descriptor if cached, translation rules, production type if known
     * - set parameters for defaults, type, configuration, descriptor, request, command line
     * - create job client
     * - if not cached: read processor descriptor, production type unless known before
     * - if not cached: set parameters for type, descriptor, request, command line
     *
     * Processor descriptors are read from distributed file system either from
     * processorname-descriptor.json (new) or from bundle-descriptor.xml (old).
     * The json files can be cached in a local directory.
     * @param submittedRequest   request parameters
     * @param commandLineParameters  command line parameters (optional)
     * @param configParameters   Calvalus configuration parametesr
     * @return  Hadoop job configuration
     * @throws IllegalArgumentException  thrown if production type or processor does not exist
     * @throws InvocationTargetException  thrown if there is an error in a translation rule
     * @throws IllegalAccessException    thrown if there is an error in a translation rule
     * @throws NoSuchMethodException    thrown if there is an error in a translation rule
     * @throws InterruptedException  thrown if access to descriptors on distributed file system fails
     * @throws IOException  thrown if access to descriptors on distributed file system fails
     */
    public CalvalusHadoopParameters collectParameters(
            Map<String, Object> submittedRequest,
            Map<String, String> commandLineParameters,
            Map<Object, Object> configParameters)
            throws IOException, InvocationTargetException, IllegalAccessException, NoSuchMethodException, InterruptedException {

        // find processor in command line or request, read cached processor descriptor if available, check authorisation

        final String processor = (commandLineParameters != null && commandLineParameters.containsKey("processor"))
                ? commandLineParameters.get("processor")
                : (submittedRequest != null && submittedRequest.containsKey("processor"))
                ? (String)submittedRequest.get("processor")
                : null;
        Map<String, Object> processorDescriptor = null;
        if (processor != null && processorDescriptorDir != null) {
            final String descriptorPath = processorDescriptorDir + "/" + processor + "-descriptor.json";
            if (new File(descriptorPath).exists()) {
                processorDescriptor = parseFeatures(descriptorPath);
                check_authorisation(processorDescriptor, processor);
                processorDescriptor.remove("processorDescriptor");
            }
        }

        // read parameter translation rules if available

        final Map<String, Object> parameterTranslationRules;
        final String translationRulePath = productionTypeDir + "/calvalus-cht-type.json";
        if (new File(translationRulePath).exists()) {
            parameterTranslationRules = parseFeatures(translationRulePath);
        } else {
            parameterTranslationRules = new HashMap<>();
        }

        // find production type in command line, request, or processor descriptor if set, read type descriptor

        String productionType = (commandLineParameters != null && commandLineParameters.containsKey("productionType"))
                ? commandLineParameters.get("productionType")
                : (submittedRequest != null && submittedRequest.containsKey("productionType"))
                ? (String)submittedRequest.get("productionType")
                : (processorDescriptor != null && processorDescriptor.containsKey("productionType"))
                ? (String)processorDescriptor.get("productionType")
                : null;
        Map<String, Object> productionTypeDef = null;
        if (productionType != null) {
            try {
                productionTypeDef = parseFeatures(productionTypeDir + "/" + productionType + "-cht-type.json");
                moveTranslationRules(productionTypeDef, parameterTranslationRules);
            } catch (FileNotFoundException e) {
                throw new IllegalArgumentException("unknown production type " + productionType);
            }
        }

        // collect parameters, set defaults, merge structures

        CalvalusHadoopParameters hadoopParameters = new CalvalusHadoopParameters();
        setHadoopDefaultParameters(hadoopParameters);
        Map<String, Object> structuredParameters = new HashMap<>();

        if (submittedRequest != null && productionType != null) {
            final Date now = new Date();
            final String productionId = Production.createId(productionType);
            hadoopParameters.set("calvalus.output.dir", String.format("/calvalus/home/%s/%s", userName, productionId));
            hadoopParameters.set("jobSubmissionDate", ISO_MILLIS_FORMAT.format(now));
        }
        hadoopParameters.set("calvalus.user", userName);
        LOG.fine("setting " + hadoopParameters.size() + " default parameters");

        // add parameters of production type, maybe translate and apply functions

        int count;
        if (productionTypeDef != null) {
            count = mergeParameters(productionTypeDef, parameterTranslationRules, hadoopParameters, structuredParameters);
            LOG.info("reading production type definition from " + "etc/" + productionType + "-cht-type.json with "
                         + count + " parameters");
            LOG.info(parameterTranslationRules.size() + " parameter translation rules");
        }

        // add parameters of config

        count = mergeParameters(configParameters, parameterTranslationRules, hadoopParameters, structuredParameters);
        LOG.info("reading calvalus configuration with " + count + " parameters");

        // add parameters of processor descriptor

        if (processorDescriptor != null) {
            count = mergeParameters(processorDescriptor, parameterTranslationRules, hadoopParameters, structuredParameters);
            LOG.info("reading processor descriptor with " + count + " parameters");
        }

        // add parameters of request

        if (submittedRequest != null) {
            count = mergeParameters(submittedRequest, parameterTranslationRules, hadoopParameters, structuredParameters);
            LOG.info("reading request with " + count + " parameters");
        }

        // add parameters of command line
        if (commandLineParameters != null && commandLineParameters.size() > 0) {
            count = mergeParameters(commandLineParameters, parameterTranslationRules, hadoopParameters, structuredParameters);
            LOG.fine("adding " + count + " command line parameters");
        }

        // create job client for user and for access to file system

        hadoopConnection.createJobClient(hadoopParameters);

        // handle case of processor descriptor that is not cached, look into remote dir, re-apply updates

        if (processorDescriptor == null) {

            if (processor != null) {

                // look for processor descriptor json

                processorDescriptor = hadoopConnection.getRemoteProcessorDescriptor(processor, userName);
                // no undeclared processors allowed
                if (processorDescriptor == null) {
                    throw new IllegalArgumentException("unknown producessor " + processor);
                }
                check_authorisation(processorDescriptor, processor);
                LOG.info("reading processor descriptor from bundle with " + processorDescriptor.size() + " parameters");
                processorDescriptor.remove("processorDescriptor");

            } else {

                // look for bundle descriptor xml

                String bundles = hadoopParameters.get("calvalus.bundles");
                String processorName = hadoopParameters.get("calvalus.l2.operator");
                if (bundles != null && processorName != null) {
                    processorDescriptor = hadoopConnection.getRemoteBundleDescriptor(bundles, processorName, userName);
                }
                if (processorDescriptor != null) {
                    LOG.info("reading bundle descriptor with " + processorDescriptor.size() + " parameters");
                }

            }

            if (processorDescriptor != null) {
                // merge production type parameters if production type revealed in remote descriptor
                if (productionType == null && processorDescriptor.containsKey("productionType")) {
                    productionType = (String) processorDescriptor.get("productionType");
                    final Date now = new Date();
                    final String productionId = Production.createId(productionType);
                    hadoopParameters.set("calvalus.output.dir", String.format("/calvalus/home/%s/%s", userName, productionId));
                    hadoopParameters.set("calvalus.user", userName);
                    hadoopParameters.set("jobSubmissionDate", ISO_MILLIS_FORMAT.format(now));
                    try {
                        productionTypeDef = parseFeatures(productionTypeDir + "/" + productionType + "-cht-type.json");
                        moveTranslationRules(productionTypeDef, parameterTranslationRules);
                        count = mergeParameters(productionTypeDef, parameterTranslationRules, hadoopParameters, structuredParameters);
                        LOG.info("reading production type definition from " + "etc/" + productionType + "-cht-type.json with "
                                    + count + " parameters");
                        LOG.info(parameterTranslationRules.size() + " parameter translation rules");
                    } catch (FileNotFoundException e) {
                        throw new IllegalArgumentException("unknown production type " + productionType);
                    }
                }
                // override with config
                mergeParameters(configParameters, parameterTranslationRules, hadoopParameters, structuredParameters);
                // override with processor descriptor
                mergeParameters(processorDescriptor, parameterTranslationRules, hadoopParameters, structuredParameters);
                // override with request
                if (submittedRequest != null) {
                    mergeParameters(submittedRequest, parameterTranslationRules, hadoopParameters, structuredParameters);
                }
                // override with command line
                if (commandLineParameters != null) {
                    mergeParameters(commandLineParameters, parameterTranslationRules, hadoopParameters, structuredParameters);
                }
            }
        }

        translateStructuredParameters(structuredParameters, parameterTranslationRules, hadoopParameters);

        // make relative output directory absolute

        final String outputDir = hadoopParameters.get("calvalus.output.dir");
        if (outputDir != null && ! outputDir.startsWith("/")) {
            hadoopParameters.set("calvalus.output.dir", "/calvalus/home/" + userName + "/" + outputDir);
        }

        // patch java options with javaOptsHook if mapreduce.jvm.add-opens-as-default is set, i.e. for Java 21

        if (Boolean.parseBoolean(hadoopParameters.get("mapreduce.jvm.add-opens-as-default"))) {
            final String javaOptsHook = hadoopParameters.get("javaOptsHook");
            final String mapJavaOpts = hadoopParameters.get("mapreduce.map.java.opts");
            final String reduceJavaOpts = hadoopParameters.get("mapreduce.reduce.java.opts");
            hadoopParameters.set("mapreduce.map.java.opts", mapJavaOpts + " " + javaOptsHook);
            hadoopParameters.set("mapreduce.reduce.java.opts", reduceJavaOpts + " " + javaOptsHook);
        }

        return hadoopParameters;
    }

    /**
     *
     * @param requestParameters
     * @param parameterTranslationRules
     * @param hadoopParameters
     * @param structuredParameters
     * @return
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     */
    private static int mergeParameters(
            Map requestParameters,
            Map<String, Object> parameterTranslationRules,
            CalvalusHadoopParameters hadoopParameters,
            Map<String, Object> structuredParameters
    )
            throws IllegalAccessException, InvocationTargetException, NoSuchMethodException
    {
        int count = 0;
        for (Object entry : requestParameters.keySet()) {
            String key = entry.toString();
            if (key.startsWith("_")) { continue; }
            final Object value = requestParameters.get(key);
            if ((value) instanceof Map) {
                final Object defaultValue = structuredParameters.get(key);
                if (defaultValue == null) {
                    structuredParameters.put(key, value);
                } else {
                    mergeFeatures((Map<String, Object>) defaultValue, (Map<String, Object>) value);
                }
            } else {
                if (key.startsWith("calvalus.hadoop.")) {
                    key = key.substring("calvalus.hadoop.".length());
                }
                translateAndInsert(key, String.valueOf(value), parameterTranslationRules, hadoopParameters);
            }
            ++count;
        }
        return count;
    }

    /**
     * Recursively merges two feature structures, updates features parameter with (keys and) values of update
     * @param features
     * @param update
     * @return
     */
    private static void mergeFeatures(Map<String, Object> features, Map<String, Object> update) {
        for (Map.Entry<String, Object> entry : update.entrySet()) {
            // recursive call only if key is in both and both are maps
            // else either add or override with update
            if (features.containsKey(entry.getKey())
                    && features.get(entry.getKey()) instanceof Map
                    && entry.getValue() instanceof Map)
            {
                mergeFeatures((Map<String, Object>) features.get(entry.getKey()), (Map<String, Object>) entry.getValue());
            } else {
                features.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private static void translateStructuredParameters(
            Map<String, Object> structuredParameters,
            Map<String, Object> parameterTranslationRules,
            CalvalusHadoopParameters hadoopParameters
    )
            throws JsonProcessingException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
    {
        for (Map.Entry<String, Object> entry : structuredParameters.entrySet()) {
            XmlMapper xmlMapper = new XmlMapper();
            final String xml = xmlMapper.writeValueAsString(entry.getValue());
            final String xmlValue = xml.substring("<LinkedHashMap>".length(), xml.length() - "</LinkedHashMap>".length());
            translateAndInsert(entry.getKey(), xmlValue, parameterTranslationRules, hadoopParameters);
        }
    }

    private static void moveTranslationRules(Map<String, Object> productionTypeDef, Map<String, Object> parameterTranslationRules) {
        for (Iterator<Map.Entry<String, Object>> entries = productionTypeDef.entrySet().iterator(); entries.hasNext(); ) {
            final Map.Entry<String, Object> entry = entries.next();
            if (entry.getKey().startsWith("_translate.")) {
                parameterTranslationRules.put(entry.getKey(), entry.getValue());
                entries.remove();
            }
        }
    }

    private void check_authorisation(Map<String, Object> processorDescriptor, String processor) 
            throws JsonProcessingException, FileNotFoundException 
    {
        List<String> authorisations = 
                (List<String>) ((Map<String, Object>) processorDescriptor.get("processorDescriptor")).get("authorisation");
        if (roleMatcher != null && ! roleMatcher.matches(authorisations)) {
            throw new FileNotFoundException(processor + " does not exist");
        }
    }

    /**
     * Converts Hadoop parameters into Hadoop JobConf, installs processor packages
     * @param hadoopParameters  Job parameters in Configuration format
     * @param hook  function to be applied on all parameters that can add parameters, e.g. a SAML token
     * @return  JobConf with Hadoop job parameters
     * @throws IllegalArgumentException  thrown if the procesor package is not found
     * @throws IOException  thrown if processor package deployment fails
     */
    public JobConf createJob(CalvalusHadoopParameters hadoopParameters, HadoopJobHook hook) throws IOException {
        // install processor bundles and calvalus and snap bundle
        JobConf jobConf = new JobConf(hadoopParameters);
        hadoopConnection.installProcessorBundles(userName, jobConf);
        final String calvalusBundle = hadoopParameters.get(JobConfigNames.CALVALUS_CALVALUS_BUNDLE);
        if (calvalusBundle != null) {
            hadoopConnection.installBundle(calvalusBundle, jobConf);
        }
        final String snapBundle = hadoopParameters.get(JobConfigNames.CALVALUS_SNAP_BUNDLE);
        if (snapBundle != null) {
            hadoopConnection.installBundle(snapBundle, jobConf);
        }
        if (hook != null) {
            hook.beforeSubmit(jobConf);
        }
        if (LOG.isLoggable(Level.FINER)) {
            printParameters("Parameterised job", jobConf);
        }
        LOG.fine("job has " + jobConf.size() + " parameters");
        return jobConf;
    }

    public static String jsonifyRequest(String requestPath) throws IOException {
        Map<String, Object> request = parseFeatures(requestPath);
        for (Map.Entry<String, Object> entry : request.entrySet()) {
            if (entry.getValue() instanceof String && ((String) entry.getValue()).startsWith("<parameters>")) {
                SimpleModule module = new SimpleModule().addDeserializer(Object.class, Issue205FixedUntypedObjectDeserializer.getInstance());
                XmlMapper xmlMapper = (XmlMapper) new XmlMapper().registerModule(module);
                Map<String, Object> content = (Map<String, Object>) xmlMapper.readValue(((String) entry.getValue()), Object.class);
                Map<String, Object> parameters = new LinkedHashMap<>();
                parameters.put("parameters", content);
                request.put(entry.getKey(), parameters);
            }
        }
        ObjectMapper jsonPrinter = new ObjectMapper();
        return jsonPrinter.writerWithDefaultPrettyPrinter().writeValueAsString(request);
    }

    public Configuration createHadoopConf(Map<String, String> commandLineParameters, Properties configParameters) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException {

        // read parameter translation rules if available

        Map<String, Object> parameterTranslationRules = EMPTY_FEATURES;
        if (productionTypeDir != null) {
            final String translationRulePath = productionTypeDir + "/calvalus-cht-type.json";
            if (new File(translationRulePath).exists()) {
                parameterTranslationRules = parseFeatures(translationRulePath);
            }
        }

        // create Hadoop job config with Hadoop defaults

        CalvalusHadoopParameters hadoopParameters = new CalvalusHadoopParameters();
        setHadoopDefaultParameters(hadoopParameters);
        Map<String, Object> structuredParameters = new HashMap<>();
        hadoopParameters.set("calvalus.user", userName);

        // add parameters of config, translate and apply function

        mergeParameters(configParameters, parameterTranslationRules, hadoopParameters, structuredParameters);
        mergeParameters(commandLineParameters, parameterTranslationRules, hadoopParameters, structuredParameters);

        return hadoopParameters;
    }

    /**
     * Read Calvalus configuration file
     */

    public static Properties collectConfigParameters(CommandLine commandLine) throws IOException {
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

    public static Properties collectConfigParameters(File calvalusConfigPath) throws IOException {
        final Properties calvalusConfig = new Properties();
        try (FileReader reader = new FileReader(calvalusConfigPath)) {
            calvalusConfig.load(reader);
        }
        return calvalusConfig;
    }


    /**
     * Convert command line into parameters
     */

    public static Map<String, String> collectCommandLineParameters(CommandLine commandLine) {
        Map<String, String> commandLineParameters = new HashMap<String, String>();
        for (Option option : commandLine.getOptions()) {
            if (!"config".equals(option.getLongOpt())
                    && !"quiet".equals(option.getLongOpt())
                    && !"debug".equals(option.getLongOpt())
                    && !"overwrite".equals(option.getLongOpt())
                    && !"async".equals(option.getLongOpt())
                    && !"status".equals(option.getLongOpt())
                    && !"cancel".equals(option.getLongOpt())
                    && !"auth".equals(option.getLongOpt())) {
                commandLineParameters.put(option.getLongOpt(), commandLine.getOptionValue(option.getLongOpt()));
            }
        }
        return commandLineParameters;
    }

    /**
     * Read request or production type definition
     */

    public static Map<String, Object> parseFeatures(String path) throws IOException {
        switch (FileUtils.getExtension(path)) {
            case ".json":
                ObjectMapper jsonParser = new ObjectMapper();
                jsonParser.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
                jsonParser.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
                return jsonParser.readValue(new File(path), VALUE_TYPE_REF);
            case ".yaml":
                ObjectMapper yamlParser = new ObjectMapper(new YAMLFactory());
                yamlParser.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
                yamlParser.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
                return yamlParser.readValue(new File(path), VALUE_TYPE_REF);
            case ".xml":
                HashMap<String, Object> parameterMap = new HashMap<String, Object>();
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

    /**
     * read one XML literal or complex content value
     */

    private static String getElementContent(Element elem, XMLOutputter xmlOutputter) throws IOException {
        List children = elem.getChildren();
        if (children.size() > 0) {
            StringWriter out = new StringWriter();
            Element complexContent = (Element) children.get(0);
            xmlOutputter.output(complexContent, out);
            return out.toString();
        } else {
            return elem.getText();
        }
    }

    /**
     * set Calvalus default parameters for Hadoop
     */

    public static void setHadoopDefaultParameters(Configuration hadoopParameters) {
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

    /**
     * Convert Calvalus parameter key and value to Hadoop parameter key and value according to production type definition
     */

    public static void translateAndInsert(
            String key, String value, Map<String, Object> parameterTranslationRules, CalvalusHadoopParameters jobConf
    )
            throws IllegalAccessException, InvocationTargetException, NoSuchMethodException
    {
        String translationKey = "_translate." + key;
        Object translations = parameterTranslationRules != null ? parameterTranslationRules.get(translationKey) : null;
        if (translations == null) {
            // no translation
            jobConf.set(key, value);
        } else if (translations instanceof String) {
            // key replacement
            LOG.fine("translating " + key + " to " + translations);
            jobConf.set((String) translations, value);
        } else {
            for (Object translation : (ArrayList<Object>) translations) {
                if (translation instanceof String) {
                    // keys replacement
                    LOG.fine("translating " + key + " to " + translation);
                    jobConf.set((String) translation, value);
                } else {
                    // function applied to value, key replacement
                    String hadoopKey = String.valueOf(((ArrayList<Object>) translation).get(0));
                    String functionName = String.valueOf(((ArrayList<Object>) translation).get(1));
                    String hadoopValue = (String) jobConf.getClass().getMethod(functionName, String.class).invoke(jobConf, value);
                    LOG.fine("translating " + key + ":" + value + " to " + hadoopKey + ":" + hadoopValue);
                    jobConf.set(hadoopKey, hadoopValue);
                }
            }
        }
    }

    /**
     * List all Hadoop job parameters on console
     */

    public static void printParameters(String header, Configuration jobConf) {
        LOG.fine(header);
        LOG.fine(String.valueOf(jobConf));
        Iterator<Map.Entry<String, String>> iterator = jobConf.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            LOG.fine(entry.getKey() + " : " + entry.getValue());
        }
    }

}