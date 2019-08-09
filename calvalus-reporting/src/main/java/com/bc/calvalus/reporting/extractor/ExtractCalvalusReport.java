package com.bc.calvalus.reporting.extractor;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.extractor.options.CLIHandlerOption;
import com.bc.wps.utilities.PropertiesWrapper;

import java.io.IOException;
import java.util.logging.Level;

/**
 * @author muhammad.bc.
 */
public class ExtractCalvalusReport {
    public static void main(String[] args) {
        try {
            PropertiesWrapper.loadConfigFile("calvalus-reporting.properties");
        } catch (IOException e) {
            CalvalusLogger.getLogger().log(Level.SEVERE, "error in GenerateCalvalusReport class", e);
        }
        CLIHandlerOption CLIHandlerOption = new CLIHandlerOption(args);

    }
}
