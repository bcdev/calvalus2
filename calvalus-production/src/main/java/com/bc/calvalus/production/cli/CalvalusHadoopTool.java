package com.bc.calvalus.production.cli;

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
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
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
        String userName = System.getProperty("user.name");
        UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(userName);
        final Configuration jobConfTemplate = new Configuration();
        JobClient jobClient = remoteUser.doAs((PrivilegedExceptionAction<JobClient>) () -> new JobClient(new JobConf(jobConfTemplate)));
        JobConf jobConf = new JobConf(new Configuration(jobClient.getConf()));

        printParameters("Hadoop defaults", jobConf);

        // set parameters by tool
        jobConf.set("calvalus.user", userName);
        jobConf.set("mapreduce.framework.name", "yarn");
        jobConf.set("fs.hdfs.impl.disable.cache", "true");
        jobConf.set("mapred.mapper.new-api", "true");
        jobConf.set("mapred.reducer.new-api", "true");
        //jobConf.set("jobSubmissionDate", ISO_MILLIS_FORMAT.format(new Date()));

        // add parameters of config, maybe translate and apply function
        for (Map.Entry<Object,Object> entry : configParameters.entrySet()) {
            translateAndInsert(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()), productionTypeDef, jobConf);
        }

        // add parameters of production type, maybe translate and apply function
        for (Map.Entry<String,Object> entry : productionTypeDef.entrySet()) {
            if (! entry.getKey().startsWith("_")) {
                translateAndInsert(entry.getKey(), String.valueOf(entry.getValue()), productionTypeDef, jobConf);
            }
        }

        // add parameters of command line, maybe translate and apply function
        for (Map.Entry<String,String> entry : commandLineParameters.entrySet()) {
            translateAndInsert(entry.getKey(), entry.getValue(), productionTypeDef, jobConf);
        }

        printParameters("Parameterised job", jobConf);



        String productionName = (String) request.get("productionName");
        String outputDir = (String) request.get("outputDir");
        //Job job = Job.getInstance(jobConf, productionName);
//        FileSystem fileSystem = jobClient.getFs();
//        ProcessorFactory.installProcessorBundles(userName, jobConf, fileSystem);
//        final Path outputPath = new Path(outputDir);
//        if (fileSystem.exists(outputPath)) {
//            //fileSystem.delete(outputPath, true);
//        }
//        FileOutputFormat.setOutputPath(job, outputPath);

        //RunningJob runningJob = jobClient.submitJob(jobConf);
    }

    private void translateAndInsert(String key, String value, Map<String, Object> productionTypeDef, JobConf jobConf)
            throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        String translationKey = "_translate." + key;
        if (! productionTypeDef.containsKey(translationKey)) {
            jobConf.set(key, value);
        } else {
            for (String translation : ((String) productionTypeDef.get(translationKey)).split(";")) {
                String hadoopKey;
                String hadoopValue;
                if (! translation.contains(",")) {
                    hadoopKey = translation.trim();
                    hadoopValue = value;
                } else {
                    String[] translationElements = translation.split(",");
                    hadoopKey = translationElements[0].trim();
                    String functionName = translationElements[1].trim();
                    hadoopValue = (String) getClass().getMethod(functionName, String.class).invoke(this, value);
                }
                jobConf.set(hadoopKey, hadoopValue);
            }
        }
    }

    private void printParameters(String header, JobConf jobConf) {
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
