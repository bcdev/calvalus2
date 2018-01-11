package com.bc.calvalus.reporting.restservice.io;

import com.bc.wps.utilities.PropertiesWrapper;
import org.junit.runner.*;
import org.junit.runner.notification.*;
import org.junit.runners.*;
import org.junit.runners.model.*;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * @author muhammad.bc.
 */
public class LoadProperties extends BlockJUnit4ClassRunner {
    private static final String CALVALUS_LOG_EXECUTE_TEST = "com.bc.calvalus.reporting.restservice.io";
    private Class<?> klass;
    private boolean executeConnection;

    /**
     * Creates a BlockJUnit4ClassRunner to run {@code klass}
     *
     * @param klass
     * @throws org.junit.runners.model.InitializationError if the test class is malformed.
     */
    public LoadProperties(Class<?> klass) throws InitializationError, IOException {
        super(klass);
        this.klass = klass;

        executeConnection = Boolean.getBoolean(CALVALUS_LOG_EXECUTE_TEST);
        if (!executeConnection) {
            System.out.println("Absolute file path to resource/log-file. Set VM param '-D" + CALVALUS_LOG_EXECUTE_TEST + "=true' to enable.");
        }
        PropertiesWrapper.loadConfigFile("conf/calvalus-reporting.properties");
        String filePath = PropertiesWrapper.get("reporting.folder.path");
        boolean exists = Paths.get(filePath).toFile().exists();
        if (!exists) {
            System.out.println("Set the absolute file path to resource/log-file, In the calvalus-reporting.properties");
            executeConnection = false;
        }
    }


    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        if (executeConnection) {
            super.runChild(method, notifier);
        } else {
            final Description description = Description.createTestDescription(klass, "allMethods. Connection tests disabled. Set VM param -D" + CALVALUS_LOG_EXECUTE_TEST + "=true to enable.");
            notifier.fireTestIgnored(description);
        }
    }
}
