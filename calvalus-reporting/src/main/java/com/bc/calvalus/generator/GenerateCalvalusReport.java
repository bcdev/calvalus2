package com.bc.calvalus.generator;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.generator.options.HandleOption;
import com.bc.wps.utilities.PropertiesWrapper;

import java.io.IOException;
import java.util.logging.Level;

/**
 * @author muhammad.bc.
 */
public class GenerateCalvalusReport {
    public static void main(String[] args) {
        try {
            PropertiesWrapper.loadConfigFile("conf/calvalus-reporting.properties");
        } catch (IOException e) {
            CalvalusLogger.getLogger().log(Level.SEVERE, "error in GenerateCalvalusReport class", e);
        }
        HandleOption handleOption = new HandleOption(args);

    }
}
