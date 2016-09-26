package com.bc.calvalus.wps.cmd;

import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;

/**
 * @author hans
 */
public class CommandLineResultHandler extends DefaultExecuteResultHandler {

    private ExecuteWatchdog watchdog;

    public CommandLineResultHandler(ExecuteWatchdog watchdog) {
        this.watchdog = watchdog;
    }

    @Override
    public void onProcessComplete(int exitValue) {
        super.onProcessComplete(exitValue);
        System.out.println("[resultHandler] The process has been successfully completed.");
    }

    @Override
    public void onProcessFailed(ExecuteException exception) {
        super.onProcessFailed(exception);
        if (watchdog != null && watchdog.killedProcess()) {
            System.err.println("[resultHandler] The process has timed out");
        } else {
            System.err.println("[resultHandler] The process has failed : " + exception.getMessage());
        }
    }
}
