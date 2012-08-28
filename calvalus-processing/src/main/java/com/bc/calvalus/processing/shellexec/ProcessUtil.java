package com.bc.calvalus.processing.shellexec;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Wrapper around Process with optional observer of lines written to stdout and stderr.
 * Starts process, waits for termination, and collects all output. An observer may be
 * used as follows:
 * <pre>
 *   p = new ProcessUtil(new ProcessUtil.OutputObserver() {
 *       public void handle(String line) { System.out.println(line); }
 *   });
 * </pre>
 * If the command requires an environment use
 * <pre>
 *   p.environment().put("somekey", "somevalue");
 * </pre>
 * before run().
 * 
 * @author Martin Boettcher
 */
@Deprecated
class ProcessUtil {

    /**
     * Interface to be implemented by observers that get callbacks for each line of process output.
     */
    public interface OutputObserver {
        void handle(String line);
    }

    private ProcessBuilder processBuilder = new ProcessBuilder();
    private StringBuffer output = new StringBuffer();
    private OutputObserver observer = null;

    /** Default constructor without observer */
    public ProcessUtil() {}

    /** Constructor with observer for callbacks */
    public ProcessUtil(OutputObserver observer) {
        this.observer = observer;
    }

    /**
     * Sets working directory of process
     * @param dir  working directory
     */
    public void directory(File dir) { processBuilder.directory(dir); }

    /**
     * Returns map of process environment for update before run call
     * @return  map of environment variables and values
     */
    public Map<String,String> environment() { return processBuilder.environment();}

    /** Returns collected output from stdout and stderr */
    public String getOutputString() { return output.toString(); }

    /**
     * Runs process for command line, collects process output, and calls observer for each line.
     * @param call  command and command line arguments
     * @return  return code of command
     * @throws IOException  if process communication fails
     * @throws InterruptedException  if process is interrupted
     */
    public int run(String ... call) throws IOException, InterruptedException {
        processBuilder.command(call);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        // read output until process terminates
        output.setLength(0);
        int c;
        int p = 0;
        while ((c = process.getInputStream().read()) != -1) {
            if (observer != null && c == '\n') {
                observer.handle(output.substring(p));
                p = output.length() + 1;
            }
            output.append((char) c);
        }
        if (observer != null && p < output.length()) {
            observer.handle(output.substring(p));
        }
        // check for termination and handle exit code
        process.waitFor();
        return process.exitValue();
    }
}
