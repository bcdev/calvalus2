package com.bc.calvalus.production.cli;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopJobHook;
import com.bc.calvalus.production.Production;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CalvalusHadoopRequestConverter {
    
    private static final SimpleDateFormat ISO_MILLIS_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    static {
        ISO_MILLIS_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
    private static final TypeReference<Map<String, Object>> VALUE_TYPE_REF = new TypeReference<Map<String, Object>>() {};
    private static Logger LOG = CalvalusLogger.getLogger();
    
    private final CalvalusHadoopConnection hadoopConnection;
    private final String userName;

    public CalvalusHadoopRequestConverter(CalvalusHadoopConnection hadoopConnection, String userName) {
        this.hadoopConnection = hadoopConnection;
        this.userName = userName;
    }

    /**
     * Parameterise Hadoop job from command line, configuration, request, bundle descriptor, production type
     */

    public JobConf createJob(String requestPath, Map<String, String> commandLineParameters, Map<Object, Object> configParameters, HadoopJobHook hook)
            throws IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, InterruptedException 
    {
        // read request and production type definition
        Map<String, Object> request = parseIntoMap(requestPath);
        String productionType = getParameter(request, "productionType", "calvalus.productionType");
        LOG.info("reading request from " + requestPath + " with type " + productionType + " and " + request.size() + " parameters");
        Map<String, Object> productionTypeDef = parseIntoMap("etc/" + productionType + "-cht-type.json");

        // create Hadoop job config with Hadoop defaults
        CalvalusHadoopParameters hadoopParameters = new CalvalusHadoopParameters();
        setHadoopDefaultParameters(hadoopParameters);

        // set parameters by tool
        final Date now = new Date();
        final String productionId = Production.createId(productionType);
        hadoopParameters.set("calvalus.output.dir", String.format(userName, productionId));
        hadoopParameters.set("calvalus.user", userName);
        hadoopParameters.set("jobSubmissionDate", ISO_MILLIS_FORMAT.format(now));
        LOG.fine("setting " + hadoopParameters.size() + " default parameters");

        // add parameters of config, maybe translate and apply function
        int count = 0;
        for (Map.Entry<Object, Object> entry : configParameters.entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (key.startsWith("calvalus.hadoop.")) {
                key = key.substring("calvalus.hadoop.".length());
            }
            translateAndInsert(key, String.valueOf(entry.getValue()), productionTypeDef, hadoopParameters);
            ++count;
        }
        LOG.info("reading .calvalus configuration with " + count + " parameters");

        // add parameters of production type, maybe translate and apply function
        count = 0;
        for (Map.Entry<String, Object> entry : productionTypeDef.entrySet()) {
            if (!entry.getKey().startsWith("_")) {
                if (entry.getValue() instanceof Map) {
                    XmlMapper xmlMapper = new XmlMapper();
                    final String xml = xmlMapper.writeValueAsString(entry.getValue());
                    final String xmlValue = xml.substring("<LinkedHashMap>".length(), xml.length() - "</LinkedHashMap>".length());
                    translateAndInsert(entry.getKey(), xmlValue, productionTypeDef, hadoopParameters);
                } else {
                    translateAndInsert(entry.getKey(), String.valueOf(entry.getValue()), productionTypeDef, hadoopParameters);
                }
                ++count;
            }
        }
        LOG.info("reading production type definition from " + "etc/" + productionType + "-cht-type.json with "
                         + count + " parameters and " + (productionTypeDef.size() - count) + " rules");

        // add parameters of request, maybe translate and apply function
        for (Map.Entry<String, Object> entry : request.entrySet()) {
            if (entry.getValue() instanceof Map) {
                XmlMapper xmlMapper = new XmlMapper();
                final String xml = xmlMapper.writeValueAsString(entry.getValue());
                final String xmlValue = xml.substring("<LinkedHashMap>".length(), xml.length() - "</LinkedHashMap>".length());
                translateAndInsert(entry.getKey(), xmlValue, productionTypeDef, hadoopParameters);
            } else {
                translateAndInsert(entry.getKey(), String.valueOf(entry.getValue()), productionTypeDef, hadoopParameters);
            }
        }

        // add parameters of command line, maybe translate and apply function
        for (Map.Entry<String, String> entry : commandLineParameters.entrySet()) {
            translateAndInsert(entry.getKey(), entry.getValue(), productionTypeDef, hadoopParameters);
        }
        LOG.fine("adding " + commandLineParameters.size() + " command line parameters");

        // create job client for user and for access to file system
        hadoopConnection.createJobClient(hadoopParameters);

        // retrieve and add parameters of processor descriptor
        Map<String, String> processorDescriptorParameters = getProcessorDescriptorParameters(hadoopParameters);
        for (Map.Entry<String, String> entry : processorDescriptorParameters.entrySet()) {
            translateAndInsert(entry.getKey(), entry.getValue(), productionTypeDef, hadoopParameters);
        }
        LOG.info("reading processor descriptor from bundle with " + processorDescriptorParameters.size() + " parameters");

        // overwrite with parameters of request, maybe translate and apply function
        for (Map.Entry<String, Object> entry : request.entrySet()) {
            if (entry.getValue() instanceof Map) {
                XmlMapper xmlMapper = new XmlMapper();
                final String xml = xmlMapper.writeValueAsString(entry.getValue());
                final String xmlValue = xml.substring("<LinkedHashMap>".length(), xml.length() - "</LinkedHashMap>".length());
                translateAndInsert(entry.getKey(), xmlValue, productionTypeDef, hadoopParameters);
            } else {
                translateAndInsert(entry.getKey(), String.valueOf(entry.getValue()), productionTypeDef, hadoopParameters);
            }
        }

        // overwrite with parameters of command line, maybe translate and apply function
        for (Map.Entry<String, String> entry : commandLineParameters.entrySet()) {
            translateAndInsert(entry.getKey(), entry.getValue(), productionTypeDef, hadoopParameters);
        }

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

    public Configuration createHadoopConf(Map<String, String> commandLineParameters, Properties configParameters) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        // create Hadoop job config with Hadoop defaults
        CalvalusHadoopParameters hadoopParameters = new CalvalusHadoopParameters();
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

    static Map<String, Object> parseIntoMap(String path) throws IOException {
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

    /**
     * read bundle descriptor job parameters
     */

    private Map<String, String> getProcessorDescriptorParameters(Configuration hadoopParameters)
            throws IOException, InterruptedException {
        String bundles = hadoopParameters.get("calvalus.bundles");
        String processor = hadoopParameters.get("calvalus.l2.operator");
        if (bundles == null || processor == null) {
            LOG.info("no bundle or no processor requested");
            return Collections.emptyMap();
        }
        return hadoopConnection.getProcessorDescriptorParameters(bundles, processor, userName);
    }

    /**
     * Convert Calvalus parameter key and value to Hadoop parameter key and value according to production type definition
     */

    private void translateAndInsert(String key, String value, Map<String, Object> productionTypeDef, CalvalusHadoopParameters jobConf)
            throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        String translationKey = "_translate." + key;
        Object translations = productionTypeDef != null ? productionTypeDef.get(translationKey) : null;
        if (translations == null) {
            jobConf.set(key, value);
        } else if (translations instanceof String) {
            LOG.fine("translating " + key + " to " + translations);
            jobConf.set((String) translations, value);
        } else {
            for (Object translation : (ArrayList<Object>) translations) {
                if (translation instanceof String) {
                    LOG.fine("translating " + key + " to " + translation);
                    jobConf.set((String) translation, value);
                } else {
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
     * Look up parameter using alternative names
     */

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

    /**
     * List all Hadoop job parameters on console
     */

    private static void printParameters(String header, Configuration jobConf) {
        LOG.fine(header);
        LOG.fine(String.valueOf(jobConf));
        Iterator<Map.Entry<String, String>> iterator = jobConf.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            LOG.fine(entry.getKey() + " : " + entry.getValue());
        }
    }

}