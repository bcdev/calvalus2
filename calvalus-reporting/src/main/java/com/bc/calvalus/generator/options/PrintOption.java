package com.bc.calvalus.generator.options;

import com.bc.calvalus.generator.extractor.Extractor;
import com.bc.wps.utilities.PropertiesWrapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;
import java.util.Properties;


public abstract class PrintOption {

    public static void printHelp(String help_info) throws IOException, URISyntaxException {
        try (PrintWriter printWriter = new PrintWriter(System.out)) {
            File file;
            URL xsltFileUrl = Extractor.class.getClassLoader().getResource(PropertiesWrapper.get("cli.resource.directory") + "/" + help_info);
            if (xsltFileUrl != null) {
                file = new File(xsltFileUrl.toURI());
            } else {
                throw new FileNotFoundException("'" + help_info + "' not found.");
            }
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
