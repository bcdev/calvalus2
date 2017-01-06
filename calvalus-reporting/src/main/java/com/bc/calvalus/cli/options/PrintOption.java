package com.bc.calvalus.cli.options;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Objects;
import java.util.Properties;


public abstract class PrintOption {

    public static void printHelp(String help_info) throws IOException {
        try (PrintWriter printWriter = new PrintWriter(System.out)) {
            File file = new File(PrintOption.class.getResource(help_info).getFile());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                printWriter.println(line);
            }
            printWriter.flush();
        }
    }

    public static void printErrorMsg(String msg) {
        try (PrintWriter printWriter = new PrintWriter(System.out)) {
            printWriter.println(String.format("Option provided is not define: %s", msg));
            printWriter.println("See 'calvalus-gen --help'.");
            printWriter.flush();
        }
    }

    public static void printMsg(String msg) {
        try (PrintWriter printWriter = new PrintWriter(System.out)) {
            printWriter.println(msg);
        }
    }

    public static Properties getBuildProperties() {
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
}
