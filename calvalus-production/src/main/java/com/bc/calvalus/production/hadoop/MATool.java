package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.WorkflowException;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.SimpleStagingService;
import com.bc.calvalus.staging.StagingService;
import org.apache.commons.cli.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;

import java.io.*;
import java.util.HashMap;
import java.util.List;

/**
 * The Match-up Analysis CLI tool.
 */
public class MATool {

    public static final String TOOL_NAME = "matool";
    public static final Options TOOL_OPTIONS = createCommandlineOptions();
    private boolean errors;
    private boolean quite;

    public static void main(String[] args) {
        new MATool().run(args);
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

        if (commandLine.hasOption('h')) {
            printHelp();
            return;
        }

        String requestPath = commandLine.getOptionValue("request", "request.xml");

        try {
            FileReader fileReader = new FileReader(requestPath);
            ProductionRequest productionRequest;
            try {
                say(String.format("Loading production request '%s'...", requestPath));
                productionRequest = loadProductionRequest(fileReader);
                say(String.format("Production request loaded, type is '%s'.", productionRequest.getProductionType()));
            } finally {
                fileReader.close();
            }

            say("Creating workflow...");
            HadoopProcessingService processingService = createProcessingService();
            MAProductionType productionType = new MAProductionType(processingService, createLocalStagingService());
            Production production = productionType.createProduction(productionRequest);
            WorkflowItem workflow = production.getWorkflow();
            say("Workflow successfully created.");

            say("Submitting workflow...");
            workflow.submit();
            say("Workflow successfully submitted.");

            while (!workflow.getStatus().getState().isDone()) {
                Thread.sleep(1000);
                processingService.updateStatuses();
                workflow.updateStatus();
                say("Workflow status: " + workflow.getStatus());
            }

        } catch (JDOMException e) {
            exit("Error: Invalid WPS XML: %s", 2, e);
        } catch (ProductionException e) {
            exit("Error: production failed: %s", 3, e);
        } catch (IOException e) {
            exit("Error: I/O problem: %s", 4, e);
        } catch (WorkflowException e) {
            exit("Error: workflow failed: %s", 5, e);
        } catch (InterruptedException e) {
            exit("Warning: workflow monitoring cancelled! Job may be still alive!", 0);
        }

        say("Workflow successfully completed.");
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

    private StagingService createLocalStagingService() throws IOException {
        return new SimpleStagingService(new File("."), 3);
    }

    private void printHelp() {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(TOOL_NAME, TOOL_OPTIONS);
    }

    public CommandLine parseCommandLine(String... args) throws ParseException {
        Parser parser = new GnuParser();
        return parser.parse(TOOL_OPTIONS, args);
    }

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
                                  .withLongOpt("request")
                                  .hasArg()
                                  .withArgName("REQ")
                                  .withDescription("Production request given as WPS-conforming XML file.")
                                  .create("r"));
        return options;
    }

    private static HadoopProcessingService createProcessingService() throws IOException {
        JobConf jobConf = new JobConf();
        //TODO make this configurable
        jobConf.set("fs.default.name", "hdfs://cvmaster00:9000");
        jobConf.set("mapred.job.tracker", "cvmaster00:9001");
        JobClient jobClient = new JobClient(jobConf);
        return new HadoopProcessingService(jobClient);
    }

    ProductionRequest loadProductionRequest(Reader reader) throws JDOMException, IOException {

        SAXBuilder saxBuilder = new SAXBuilder();
        Document document = saxBuilder.build(reader);
        Element executeElement = document.getRootElement();

        Namespace wps = executeElement.getNamespace("wps");
        Namespace ows = executeElement.getNamespace("ows");
        Namespace xlink = executeElement.getNamespace("xlink");

        Element dataInputs = executeElement.getChild("DataInputs", wps);
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

        return new ProductionRequest(MAProductionType.NAME,
                                     System.getProperty("user.name", "anonymous"),
                                     parameterMap);
    }
}
