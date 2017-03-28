package com.bc.calvalus.extractor.options;

import java.io.PrintWriter;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


abstract class PrintOption {

    private static final String CMD_LINE_SYNTAX = "generate-calvalus-report [commands]";
    private static final String CMD_LINE_START = "generate-calvalus-report start";
    private static final String CMD_LINE_STOP = "generate-calvalus-report stop";
    private static final boolean AUTO_USAGE = true;
    private static final String HEADER = "Options";

    static void printHelp() {
        Options mainOptions = createMainOptions();
        HelpFormatter helpFormatter = new HelpFormatter();
        String footer = "\nCommands:\n" +
                "start   To start service\n" +
                "stop    To stop service\n\n" +
                "Run    'generate-calvalus-report COMMAND --help' for more information on a command.";
        helpFormatter.printHelp(CMD_LINE_SYNTAX, HEADER, mainOptions, footer, AUTO_USAGE);
    }

    static void printErrorMsg(String msg) {
        try (PrintWriter printWriter = new PrintWriter(System.out)) {
            printWriter.println(String.format("Option provided is not define %s", msg));
            printWriter.println("See 'generate-calvalus-report --help'.");
            printWriter.flush();
        }
    }

    static void printMsg(String msg) {
        try (PrintWriter printWriter = new PrintWriter(System.out)) {
            printWriter.println(msg);
        }
    }

    void printStartHelp() {
        Options startHelpOptions = createStartHelpOptions();
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(CMD_LINE_START, HEADER, startHelpOptions, "", AUTO_USAGE);
    }

    void printStopHelp() {
        Options options = new Options();
        options.addOption(Option.builder("f").longOpt("force").desc("To stop the service immediately").build());
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(CMD_LINE_STOP, HEADER, options, "", AUTO_USAGE);
    }

    Options createAllOptions() {
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("Print usage").build());
        options.addOption(Option.builder("v").longOpt("version").desc("Print version information and quit").build());
        options.addOption(Option.builder("f").longOpt("force").desc("To stop the service immediately").build());
        options.addOption(Option.builder("i").longOpt("interval").argName("Start").hasArg(true).desc("Set time interval or leave it blank to use default value from the configuration file").build());
        options.addOption(Option.builder("o").longOpt("output-file-path").hasArg(true).desc("Location where to save the extracted history").build());
        return options;
    }


    static Options createMainOptions() {
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("Print usage").build());
        options.addOption(Option.builder("v").longOpt("version").desc("Print version information and quit").build());
        return options;
    }

    private Options createStartHelpOptions() {
        Options options = new Options();
        options.addOption(Option.builder("i").longOpt("interval").hasArg(true).desc("Time interval or set the default in configuration file").build());
        options.addOption(Option.builder("o").longOpt("output-file-path").hasArg(true).desc("Location where to save the extracted history").build());
        return options;
    }
}
