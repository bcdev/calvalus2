package com.bc.calvalus.processing.shellexec;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.NoSuchElementException;

/**
 * A test runner to run unit-level tests that require a running standalone Hadoop on localhost.
 * Checks for port 9000 on localhost served by HDFS data service to verify that hadoop is up.
 *
 * @author Boe
 */
public class HadoopStandaloneAndAccountTestRunner extends BlockJUnit4ClassRunner {

    public HadoopStandaloneAndAccountTestRunner(Class<?> testClass) throws InitializationError {
        super(testClass);
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        try {
            // check whether local hadoop HDFS server is running
            new Socket(InetAddress.getByName("localhost"), 9000);
            // check whether user hadoop is known on this host for installation of seadas
            assertAccount("hadoop");
            // perform test
            super.runChild(method, notifier);
        } catch (UnknownHostException e) {
            System.err.println("test skipped, localhost not configured");
        } catch (NoSuchElementException e) {
            System.err.println("test skipped, " + e.getMessage());
        } catch (IOException e) {
            // inform about reason for skipping test
            System.err.println("test skipped, local Hadoop not running or failed to read /etc/passwd");
            System.err.println("start Hadoop with:");
            System.err.println("  export HADOOP_CONF_DIR=/.../calvalus/calvalus-processing/src/test/resources/mini-conf");
            System.err.println("  /.../hadoop-0.20.2/bin/hadoop namenode -format");
            System.err.println("  /.../hadoop-0.20.2/bin/start-all.sh");
            System.err.println("  sleep 30");
        }
    }

    private void assertAccount(String account) throws IOException{
        BufferedReader in = new BufferedReader(new FileReader("/etc/passwd"));
        for (String line = in.readLine(); line != null; line = in.readLine()) {
            if (line.startsWith(account)) {
                return;
            }
        }
        throw new NoSuchElementException("unknown user account " + account + " on localhost");
    }
}
