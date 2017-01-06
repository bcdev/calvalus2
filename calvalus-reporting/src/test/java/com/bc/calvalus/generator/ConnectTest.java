package com.bc.calvalus.generator;

import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.io.IOException;

/**
 * @author muhammad.bc
 */
public class ConnectTest extends BlockJUnit4ClassRunner {
    public static final String CALVALUS_LOG_EXECUTE_TEST = "calvalus.log.execute.test";
    private Class<?> klass;
    private boolean executeConnection;

    public ConnectTest(Class<?> klass) throws InitializationError, IOException {
        super(klass);
        this.klass = klass;

        executeConnection = Boolean.getBoolean(CALVALUS_LOG_EXECUTE_TEST);
        if (!executeConnection) {
            System.out.println("Connection test disabled. Set VM param '-D" + CALVALUS_LOG_EXECUTE_TEST + "=true' to enable.");
        }
        boolean checkConnection = TestUtils.checkConnection();
        if (!checkConnection) {
            System.out.println("Connection test disabled either connection.properties file not found or network connect.\n" +
                    "Please confirm the file and the url.");
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
