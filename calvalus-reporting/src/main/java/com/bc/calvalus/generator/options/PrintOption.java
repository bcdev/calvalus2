package com.bc.calvalus.generator.options;

import java.io.PrintWriter;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


abstract class PrintOption {


    private static final String CMD_LINE_SYNTAX = "generate-calvalus-report";
    private static final boolean AUTO_USAGE = true;
    private static final String HEADER = "Options";

    static void printHelp(Options options) {
        HelpFormatter helpFormatter = new HelpFormatter();
        String footer = "Commands:\n" +
                "Run    'generate-calvalus-report COMMAND --help' for more information on a command.";
        helpFormatter.printHelp(CMD_LINE_SYNTAX, HEADER, options, footer, AUTO_USAGE);
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
        try {
            Options startHelpOptions = createStartHelpOptions();
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp(CMD_LINE_SYNTAX, HEADER, startHelpOptions, "", AUTO_USAGE);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    Options createAllOptions() throws ParseException {
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("Print usage").build());
        options.addOption(Option.builder("v").longOpt("version").desc("Print version information and quit").build());
        options.addOption(Option.builder("i").longOpt("interval").hasArg(true).desc("Time interval or set the default in configuration file").build());
        options.addOption(Option.builder("o").longOpt("output-file-path").hasArg(true).desc("Location to save the extracted history").build());
        return options;
    }

    private Options createStartHelpOptions() throws ParseException {
        Options options = new Options();
        options.addOption(Option.builder("i").longOpt("interval").hasArg(true).desc("Time interval or set the default in configuration file").build());
        options.addOption(Option.builder("o").longOpt("output-file-path").hasArg(true).desc("Location to save the extracted history").build());
        return options;
    }


}
