package com.bc.calvalus.generator.options;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.Properties;


abstract class PrintOption {

    static void printHelp(String display) {
        try (PrintWriter printWriter = new PrintWriter(System.out)) {
            printWriter.println(display);
            printWriter.flush();
        }
    }

    public static void printErrorMsg(String msg) {
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

    static Properties getBuildProperties() {
        Properties properties = new Properties();
        try (InputStream resourceAsStream = PrintOption.class.getResourceAsStream("build-info.properties")) {
            if (Objects.nonNull(resourceAsStream)) {
                properties.load(resourceAsStream);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    final static String HELP_INFO = "<generate-calvalus-report> --help\n" +
            "Usage: generate-calvalus-report [COMMAND] [OPTION\n" +
            "       generate-calvalus-report [ --help | -v | --version ]\n" +
            "\n" +
            "Options:\n" +
            "\n" +
            "  -h, --help                            Print usage\n" +
            "  -i, --interval                        Time interval\n" +
            "  -o, --output-file-path                Location to save the generate report\n" +
            "  -v, --version                         Print version information and quit\n" +
            "\n" +
            "Commands:\n" +
            "    start            Start generating with the default of 30 minutes\n" +
            "\n" +
            "Run 'generate-calvalus-report COMMAND --help' for more information on a command.";

    final static String HELP_START = "Usage:  generate-calvalus-report start [OPTIONS]\n" +
            "\n" +
            "Start Interval\n" +
            "\n" +
            "Options:\n" +
            "  -i, --interval             Interval of time to generate report\n" +
            "  -o, --output-file-path     Location to save the generated report";
}
