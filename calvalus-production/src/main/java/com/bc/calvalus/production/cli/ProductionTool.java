package com.bc.calvalus.production.cli;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.*;
import com.bc.calvalus.production.hadoop.HadoopProductionServiceFactory;
import org.apache.commons.cli.*;
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
 */
public class ProductionTool {

    private static final String TOOL_NAME = "cpt";

    private static final String BEAM_BUNDLE = "beam-4.10-SNAPSHOT";
    private static final String CALVALUS_BUNDLE = "calvalus-0.3-201108";

    private static final Options TOOL_OPTIONS = createCommandlineOptions();
    public static final String DEFAULT_CONFIG_PATH = new File(ProductionServiceConfig.getUserAppDataDir(), "calvalus.config").getPath();
    private boolean errors;
    private boolean quite;

    public static void main(String[] args) {
        new ProductionTool().run(args);
    }

    private void run(String[] args) {
        CommandLine commandLine;
        try {
            commandLine = parseCommandLine(args);
        } catch (ParseException e) {
            exit("error: " + e.getMessage(), 1);
            printHelp();
            return;
        }

        errors = commandLine.hasOption("errors");
        quite = commandLine.hasOption("quite");

        String[] requestPaths = commandLine.getArgs();
        if (commandLine.hasOption('h') || requestPaths.length != 1) {
            printHelp();
            return;
        }
        String requestPath = requestPaths[0];

        Map<String, String> defaultConfig = new HashMap<String, String>();
        defaultConfig.put("calvalus.hadoop.fs.default.name", "hdfs://cvmaster00:9000");
        defaultConfig.put("calvalus.hadoop.mapred.job.tracker", "cvmaster00:9001");
        defaultConfig.put("calvalus.calvalus.bundle", commandLine.getOptionValue("calvalus", CALVALUS_BUNDLE));
        defaultConfig.put("calvalus.beam.bundle", commandLine.getOptionValue("beam", BEAM_BUNDLE));

        try {
            String configFile = commandLine.getOptionValue("config", DEFAULT_CONFIG_PATH);
            say(String.format("Loading Calvalus configuration '%s'...", configFile));
            Map<String, String> config = ProductionServiceConfig.loadConfig(new File(configFile), defaultConfig);
            say("Configuration loaded.");

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
            if (production.getProcessingStatus().getState() != ProcessState.COMPLETED) {
                exit("Error: production did not complete normally: " + production.getProcessingStatus().getMessage(), 10);
            }
        } catch (JDOMException e) {
            exit("Error: Invalid WPS XML: %s", 2, e);
        } catch (ProductionException e) {
            exit("Error: production failed: %s", 3, e);
        } catch (IOException e) {
            exit("Error: I/O problem: %s", 4, e);
        } catch (InterruptedException e) {
            exit("Warning: workflow monitoring cancelled! Job may be still alive!", 0);
        }
    }

    private void exit(String format, int exitCode, Throwable error) {
        String message = String.format(format, error.getMessage());
        System.err.println(message);
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
                                  .withDescription("The name of the Calvalus software bundle used for the production. Defaults to '" + CALVALUS_BUNDLE + "'")
                                  .create("C"));
        options.addOption(OptionBuilder
                                  .withLongOpt("beam")
                                  .hasArg()
                                  .withArgName("NAME")
                                  .withDescription("The name of the BEAM software bundle used for the production. Defaults to '" + BEAM_BUNDLE + "'.")
                                  .create("B"));
        options.addOption(OptionBuilder
                                  .withLongOpt("conf")
                                  .hasArg()
                                  .withArgName("FILE")
                                  .withDescription("The Calvalus configuration file (Java properties format). Defaults to '" + DEFAULT_CONFIG_PATH + "'.")
                                  .create("c"));
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
