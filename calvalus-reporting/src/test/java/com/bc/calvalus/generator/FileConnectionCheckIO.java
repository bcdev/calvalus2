package com.bc.calvalus.generator;

import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author muhammad.bc.
 */
public class FileConnectionCheckIO extends BlockJUnit4ClassRunner {
    public static final String CALVALUS_LOG_DIR_EXECUTE_TEST = "calvalus.extractor.dir.connect.execute.test";
    private Class<?> klass;
    private boolean checkConnection;
    private boolean executeDirectory;

    /**
     * Creates a BlockJUnit4ClassRunner to run {@code klass}
     *
     * @param klass
     * @throws InitializationError if the test class is malformed.
     */
    public FileConnectionCheckIO(Class<?> klass) throws InitializationError {
        super(klass);
        this.klass = klass;

        executeDirectory = Boolean.getBoolean(CALVALUS_LOG_DIR_EXECUTE_TEST);

        if (!executeDirectory) {
            System.out.println("Connection test disabled. Set VM param '-D" + CALVALUS_LOG_DIR_EXECUTE_TEST + "=true' to enable.");
        }

        executeDirectory = checkPath();
        checkConnection = TestUtils.checkConnection();

        if (!executeDirectory && !checkConnection) {
            System.out.println("Connection test disabled either connection.properties file not found or network connect.\n" +
                    "Please confirm the file and the url.");
            executeDirectory = false;
            checkConnection = false;
        }
    }

    private boolean checkPath() {
        Path path = Paths.get(TestUtils.getSaveLocation());
        File file = path.toFile();
        return file.isDirectory() || file.exists();
    }


    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        if (executeDirectory && checkConnection) {
            super.runChild(method, notifier);
        } else {
            final Description description = Description.createTestDescription(klass, "allMethods. Connection and File tests disabled. Set VM param -D" + CALVALUS_LOG_DIR_EXECUTE_TEST + "=true to enable.");
            notifier.fireTestIgnored(description);
        }
    }
}
