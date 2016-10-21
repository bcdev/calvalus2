package com.bc.calvalus.wps.cmd;

import com.bc.wps.utilities.WpsLogger;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
class CommandLineResultHandler extends DefaultExecuteResultHandler {

    private Logger logger = WpsLogger.getLogger();
    private ExecuteWatchdog watchdog;

    CommandLineResultHandler(ExecuteWatchdog watchdog) {
        this.watchdog = watchdog;
    }

    @Override
    public void onProcessComplete(int exitValue) {
        super.onProcessComplete(exitValue);
        logger.info("[resultHandler] The command line has been successfully executed.");
    }

    @Override
    public void onProcessFailed(ExecuteException exception) {
        super.onProcessFailed(exception);
        if (watchdog != null && watchdog.killedProcess()) {
            logger.log(Level.SEVERE, "[resultHandler] The process has timed out");
        } else {
            logger.log(Level.SEVERE, "[resultHandler] The process has failed : " + exception.getMessage());
        }
    }
}
