package com.bc.calvalus.reporting.urban;

import com.bc.calvalus.commons.CalvalusLogger;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class WpsConnection {
    static final Logger LOGGER = CalvalusLogger.getLogger();
    private final UrbanTepReporting reporter;
    JSch jsch = new JSch();
    ChannelExec channel = null;
    Session session = null;
    BufferedReader in = null;
    String cursor;

    WpsConnection(UrbanTepReporting reporter) {
        this.reporter = reporter;
        JSch.setConfig("StrictHostKeyChecking", "no");
        JSch.setConfig("HashKnownHosts", "yes");
    }

    /**
     * Runs forever in main thread of application after initialisation
     */
    void run() {
        cursor = reporter.getConfig().getProperty("reporting.wps.cursor");
        while (true) {
            try {
                LOGGER.info("connecting to WPS reports at " + reporter.getConfig().getProperty("reporting.wps.host"));
                connect();
                LOGGER.info("listening for new records from WPS ...");
                while (true) {
                    // wait for and read next record from WPS finished jobs report
                    String line = in.readLine();
                    handleLine(line);
                }
            } catch (Exception e) {
                LOGGER.warning("connection to WPS reports at " + reporter.getConfig().getProperty("reporting.wps.host") + " failed: " + e.getMessage() + " - sleeping ...");
                disconnect();
                // sleeps for one minute before retrying after failure
                try {
                    Thread.currentThread().wait(60 * 1000);
                } catch (InterruptedException ignore) {}
                drainTimer();
                LOGGER.info("queue drained and renewed");
            }
        }
    }

    void handleLine(String line) {
        if (line.startsWith("#")) {
            return;
        }
        // UrbanTepPortal  job_1495452880837_9023  2017-06-06T09:30:07Z    Dakar June 6b   20170606093001_L3_1f3f3fa2514cba        COMPLETED       www.brockmann-consult.de/bc-wps/wps/calvalus?Service=WPS&Request=GetStatus&JobId=job_1495452880837_9023
        String[] elements = line.split("\t");
        Report report = new Report(reporter,
                                   elements[1], // job
                                   elements[2], // timestamp
                                   elements[3], // name
                                   elements[4], // requestId
                                   elements[5], // requestStatus
                                   elements[6]  // uri
        );
        if (reporter.getStatusHandler().isHandled(report.job)) {
            LOGGER.info("skipping " + report.job);
        } else if (cursor != null && report.creationTime.compareTo(cursor) < 0) {
            LOGGER.info("skipping " + report.job + " with " + report.creationTime + " before cursor " + cursor);
        } else {
            LOGGER.info("record " + report.job + " received");
            reporter.getTimer().execute(report);
            reporter.getStatusHandler().setRunning(report.job, report.creationTime);
        }
    }

    void connect() throws JSchException, IOException {
        jsch.addIdentity(reporter.getConfig().getProperty("reporting.wps.keypath"));
        session = jsch.getSession(reporter.getConfig().getProperty("reporting.wps.user"), reporter.getConfig().getProperty("reporting.wps.host"), 22);
        session.connect();
        channel = (ChannelExec) session.openChannel("exec");
        channel.setCommand(String.format("tail -%sf %s", reporter.getConfig().getProperty("reporting.wps.backlog", "100"), reporter.getConfig().getProperty("reporting.wps.report")));
        channel.connect();
        in = new BufferedReader(new InputStreamReader(channel.getInputStream(), "utf-8"));
    }

    private void disconnect() {
        if (in != null) {
            try {
                in.close();
            } catch (IOException ignore) {
            }
            in = null;
        }
        if (channel != null) {
            channel.disconnect();
            channel = null;
        }
        if (session != null) {
            session.disconnect();
            session = null;
        }
    }

    private void drainTimer() {
        reporter.getTimer().shutdownNow();
        try {
            reporter.getTimer().awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {}
        reporter.getStatusHandler().running.clear();
        reporter.getStatusHandler().failed.clear();
        reporter.setTimer(new ScheduledThreadPoolExecutor(1));
    }
}
