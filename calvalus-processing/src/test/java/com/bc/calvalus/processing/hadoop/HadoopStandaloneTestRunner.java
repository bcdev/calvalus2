package com.bc.calvalus.processing.hadoop;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * A test runner to run unit-level tests that require a running standalone Hadoop on localhost.
 * Checks for port 9000 on localhost served by HDFS data service to verify that hadoop is up.
 *
 * @author Boe
 */
public class HadoopStandaloneTestRunner extends BlockJUnit4ClassRunner {

    public HadoopStandaloneTestRunner(Class<?> testClass) throws InitializationError {
        super(testClass);
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        try {
            // check whether local hadoop HDFS server is running
            new Socket(InetAddress.getByName("localhost"), 9000);
            // perform test
            super.runChild(method, notifier);
        } catch (UnknownHostException e) {
            System.err.println("test skipped, localhost not configured");
            notifier.fireTestIgnored(describeChild(method));
        } catch (IOException e) {
            // inform about reason for skipping test
            System.err.println("test skipped, local Hadoop not running");
            System.err.println("start Hadoop with:");
            System.err.println("  export HADOOP_CONF_DIR=/.../calvalus/calvalus-processing/src/test/resources/mini-conf");
            System.err.println("  /.../hadoop-0.20.2/bin/hadoop namenode -format");
            System.err.println("  /.../hadoop-0.20.2/bin/start-all.sh");
            System.err.println("  sleep 30");
            notifier.fireTestIgnored(describeChild(method));
        }
    }
}
