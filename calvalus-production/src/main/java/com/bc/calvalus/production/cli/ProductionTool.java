package com.bc.calvalus.production.cli;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.*;
import com.bc.calvalus.production.hadoop.HadoopProductionServiceFactory;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;

import java.io.*;
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
 *
 * -B,--beam &lt;NAME&gt;       The name of the BEAM software bundle used for the
 *                        production. Defaults to 'beam-4.10-SNAPSHOT'.
 * -c,--conf &lt;FILE&gt;       The Calvalus configuration file (Java properties
 *                        format). Defaults to 'null'.
 * -C,--calvalus &lt;NAME&gt;   The name of the Calvalus software bundle used for
 *                        the production. Defaults to 'calvalus-0.3-201108'
 * -d,--deploy &lt;JARS&gt;     The Calvalus JARs to be deployed to HDFS. Use the
 *                        colon ':' to separate multiple JAR paths.
 * -e,--errors            Print full Java stack trace on exceptions.
 * -h,--help              Prints out usage help.
 * -q,--quite             Quite mode, only minimum console output.
 *
 * </pre>
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
            exit("error: " + e.getMessage(), 1);
            printHelp();
            return;
        }

        errors = commandLine.hasOption("errors");
        quite = commandLine.hasOption("quite");

        if (commandLine.hasOption('h')) {
            printHelp();
            return;
        }

        List argList = commandLine.getArgList();
        if (argList.size() == 0 && !commandLine.hasOption("deploy")) {
            exit("Error: Missing argument REQUEST. Use option -h for usage.", -1);
        }
        if (argList.size() > 1) {
            exit("Error: Too many arguments. Use option -h for usage.", -1);
        }
        String requestPath = argList.size() == 1 ? (String) argList.get(0) : null;

        Map<String, String> defaultConfig = new HashMap<String, String>();
        defaultConfig.put("calvalus.hadoop.fs.default.name", "hdfs://cvmaster00:9000");
        defaultConfig.put("calvalus.hadoop.mapred.job.tracker", "cvmaster00:9001");
        defaultConfig.put("calvalus.calvalus.bundle", commandLine.getOptionValue("calvalus", DEFAULT_CALVALUS_BUNDLE));
        defaultConfig.put("calvalus.beam.bundle", commandLine.getOptionValue("beam", DEFAULT_BEAM_BUNDLE));

        try {
            String configFile = commandLine.getOptionValue("config", DEFAULT_CONFIG_PATH);
            say(String.format("Loading Calvalus configuration '%s'...", configFile));
            Map<String, String> config = ProductionServiceConfig.loadConfig(new File(configFile), defaultConfig);
            say("Configuration loaded.");

            if (commandLine.hasOption("deploy")) {
                deployCalvalusSoftware(commandLine.getOptionValue("deploy"), config);
            }

            if (requestPath == null) {
                return;
            }

            HadoopProductionServiceFactory productionServiceFactory = new HadoopProductionServiceFactory();
            ProductionService productionService = productionServiceFactory.create(config, ProductionServiceConfig.getUserAppDataDir(), new File("."));

            FileReader requestReader = new FileReader(requestPath);
            ProductionRequest request;
            try {
                say(String.format("Loading production request '%s'...", requestPath));
                request = loadProductionRequest(requestReader);
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
        }
    }

    private Production orderProduction(ProductionService productionService, ProductionRequest request) throws ProductionException, InterruptedException {
        say("Ordering production...");
        ProductionResponse productionResponse = productionService.orderProduction(request);
        Production production = productionResponse.getProduction();
        say("Production successfully ordered.");
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
            say("Production completed.");
        } else {
            exit("Error: Production did not complete normally: " + production.getProcessingStatus().getMessage(), 2);
        }
        return production;
    }

    private void stageProduction(ProductionService productionService, Production production) throws ProductionException, InterruptedException {
        say("Staging results...");
        productionService.stageProductions(production.getId());
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

    private void deployCalvalusSoftware(String sourcePathsString, Map<String, String> config) {
        Path bundlePath = new Path("/calvalus/software/0.5/" + config.get("calvalus.calvalus.bundle"));
        deploy(sourcePathsString, bundlePath, config);
    }

    private void deploy(String sourcePathsString, Path bundlePath, Map<String, String> config) {

        String[] parts = sourcePathsString.split(":");
        Path[] sourcePaths = new Path[parts.length];
        for (int i = 0; i < parts.length; i++) {
            sourcePaths[i] = new Path(parts[i]);
        }

        // check all sources are there
        for (Path path : sourcePaths) {
            if (!new File(path.toString()).isFile()) {
                exit("Error: Local file does not exist: " + path, 21);
            }
        }

        try {
            deploy(bundlePath, sourcePaths, config);
        } catch (IOException e) {
            exit("Error: Failed to deploy file to HDFS", 22, e);
        }
    }

    private void deploy(Path bundlePath, Path[] sourcePaths, Map<String, String> config) throws IOException {
        say("Deploying " + sourcePaths.length + " local file(s) to HDFS...");
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("fs.default.name", config.get("calvalus.hadoop.fs.default.name"));
        FileSystem fs = FileSystem.get(hadoopConfig);
        Path destinationPath = fs.makeQualified(bundlePath);
        for (Path sourcePath : sourcePaths) {
            say("Copying " + sourcePath + " --> " + destinationPath);
        }
        copyFromLocalFile(fs, sourcePaths, destinationPath);
        say("Files deployed.");
    }

    private void copyFromLocalFile(FileSystem fs, Path[] sourcePaths, Path destinationPath) throws IOException {
        boolean overwrite = true;
        boolean delSrc = false;
        fs.copyFromLocalFile(delSrc, overwrite, sourcePaths, destinationPath);
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
                                  .create("h"));
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
                                  .withLongOpt("conf")
                                  .hasArg()
                                  .withArgName("FILE")
                                  .withDescription("The Calvalus configuration file (Java properties format). Defaults to '" + DEFAULT_CONFIG_PATH + "'.")
                                  .create("c"));
        options.addOption(OptionBuilder
                                  .withLongOpt("deploy")
                                  .hasArg()
                                  .withArgName("JARS")
                                  .withDescription("The Calvalus JARs to be deployed to HDFS. Use the colon ':' to separate multiple JAR paths.")
                                  .create("d"));
        return options;
    }

    ProductionRequest loadProductionRequest(Reader reader) throws JDOMException, IOException {

        SAXBuilder saxBuilder = new SAXBuilder();
        Document document = saxBuilder.build(reader);
        Element executeElement = document.getRootElement();

        Namespace wps = executeElement.getNamespace("wps");
        Namespace ows = executeElement.getNamespace("ows");
        Namespace xlink = executeElement.getNamespace("xlink");

        String processIdentifier = executeElement.getChildText("Identifier", ows);

        Element dataInputs = executeElement.getChild("DataInputs", wps);
        @SuppressWarnings({"unchecked"})
        List<Element> inputElements = (List<Element>) dataInputs.getChildren("Input", wps);

        HashMap<String, String> parameterMap = new HashMap<String, String>();

        for (Element inputElement : inputElements) {
            String parameterName = inputElement.getChildText("Identifier", ows);

            Element dataElement = inputElement.getChild("Data", wps);
            String parameterValue = dataElement.getChildText("LiteralData", wps);
            if (parameterValue == null) {
                Element complexDataElement = dataElement.getChild("ComplexData", wps);
                if (complexDataElement != null) {
                    StringWriter out = new StringWriter();
                    Element complexContent = (Element) complexDataElement.getChildren().get(0);
                    new org.jdom.output.XMLOutputter().output(complexContent, out);
                    parameterValue = out.toString();
                } else {
                    Element referenceElement = dataElement.getChild("Reference", wps);
                    if (referenceElement != null) {
                        parameterValue = referenceElement.getAttributeValue("href", xlink);
                    }
                }
            }

            if (parameterValue != null) {
                parameterMap.put(parameterName, parameterValue);
            }
        }

        return new ProductionRequest(processIdentifier,
                                     System.getProperty("user.name", "anonymous"),
                                     parameterMap);
    }


}
