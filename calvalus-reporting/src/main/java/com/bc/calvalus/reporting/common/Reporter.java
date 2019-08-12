package com.bc.calvalus.reporting.common;

import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author hans
 */
public interface Reporter {
    String getName();
    Properties getConfig();
    StatusHandler getStatusHandler();
    ScheduledExecutorService getExecutorService();
    void setExecutorService(ScheduledExecutorService executorService);

    /**
     * Called by Report runnable that is queued in scheduled worker
     *
     * @param report Report object to be processed
     */
    void process(Report report);
}
