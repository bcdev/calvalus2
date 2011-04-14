package com.bc.calvalus.experiments.processing;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

/**
 * A test runner that is used to run unit-level tests that require a running Hadoop cluster.
 *
 * @author Norman Fomferra
 */
public class UnixTestRunner extends BlockJUnit4ClassRunner {

    public UnixTestRunner(Class<?> testClass) throws InitializationError {
        super(testClass);
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        if (isMac() || isUnix()) {
            super.runChild(method, notifier);
        } else {
            System.err.println("Test requires a Unix system.");
            notifier.fireTestIgnored(describeChild(method));
        }
    }

    public static boolean isMac() {
        String os = System.getProperty("os.name").toLowerCase();
        return (os.indexOf("mac") >= 0);
    }

    public static boolean isUnix() {
        String os = System.getProperty("os.name").toLowerCase();
        return (os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0);
    }
}
