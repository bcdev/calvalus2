package com.bc.calvalus.experiments.processing;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A test runner that is used to run unit-level tests that require a running Hadoop cluster.
 * <pre>
 <![CDATA[
  	<profiles>
        <profile>
            <id>hadoop</id>
            <activation>
                <activeByDefault>true</activeByDefault>
            </activation>
			<properties>
			    <calvalus.testrunner.hadoopAvailable>true</calvalus.testrunner.hadoopAvailable>
			</properties>
        </profile>
    </profiles>
 ]]>
 * </pre>
 * @author Norman Fomferra
 */
public class HadoopTestRunner extends BlockJUnit4ClassRunner {

    public static final String CALVALUS_HADOOP_AVAILABLE = "calvalus.testrunner.hadoopAvailable";
    private boolean hadoopAvailable = Boolean.getBoolean(CALVALUS_HADOOP_AVAILABLE);

    public HadoopTestRunner(Class<?> testClass) throws InitializationError {
        super(testClass);
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        if (hadoopAvailable) {
            super.runChild(method, notifier);
        } else {
            System.err.println("Hadoop cluster not running.");
            System.err.println(MessageFormat.format("Please specify system property ''{0}''", CALVALUS_HADOOP_AVAILABLE));
            notifier.fireTestIgnored(describeChild(method));
        }
    }
}
