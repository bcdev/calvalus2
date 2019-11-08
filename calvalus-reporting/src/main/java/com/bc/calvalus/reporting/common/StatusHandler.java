package com.bc.calvalus.reporting.common;

import com.bc.calvalus.commons.CalvalusLogger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class StatusHandler {

    static final Logger LOGGER = CalvalusLogger.getLogger();
    private final Reporter reporter;
    private final Map<String, String> reportMap = new HashMap<>();
    private BufferedWriter reportWriter;
    private File statusFile = null;
    private String historyDate = null;

    public List<String> running = new ArrayList<>();
    public List<String> failed = new ArrayList<>();
    int preexisting = 0;

    public StatusHandler(Reporter reporter) {
        this.reporter = reporter;
    }

    public void initReport() throws IOException {
        File reportFile = new File(reporter.getConfig().getProperty("name", reporter.getName()) + ".report");
        try {
            if (reportFile.exists()) {
                try (BufferedReader in = new BufferedReader(new FileReader(reportFile))) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        if (line.startsWith("#")) {
                            continue;
                        }
                        String[] elements = line.split("\t");
                        reportMap.put(elements[0], elements[1]);
                        if (historyDate == null) {
                            historyDate = elements[1];
                        }
                    }
                }
                reportWriter = new BufferedWriter(new FileWriter(reportFile, true));
            } else {
                historyDate = "start of service";
                reportWriter = new BufferedWriter(new FileWriter(reportFile));
            }
        } catch (IOException e) {
            throw new IOException("failed to read report file from " + reportFile + ": " + e.getMessage(), e);
        }
        preexisting = reportMap.entrySet().size();
        writeStatus();
    }


    public boolean isHandled(String job) {
        return reportMap.containsKey(job);
    }

    public synchronized void setRunning(String job, String finishingTime) {
        String name = job + "\t" + finishingTime;
        failed.remove(name);
        running.add(name);
        writeStatus();
    }

    public synchronized void setFailed(String job, String finishingTime) {
        String name = job + "\t" + finishingTime;
        failed.add(name);
        running.remove(name);
        writeStatus();
    }

    public synchronized void setHandled(String job, String finishingTime) {
        try {
            reportWriter.append(job).append("\t").append(finishingTime).append("\n").flush();
        } catch (IOException e) {
            LOGGER.warning("failed writing instance log " + reporter.getConfig().getProperty("name",
                                                                                             reporter.getName()) + ".report: " + e.getMessage());
        }
        reportMap.put(job, finishingTime);
        running.remove(job + "\t" + finishingTime);
        writeStatus();
    }

    // synchronized to prevent frequently-thrown ConcurrentModificationException
    private synchronized void writeStatus() {
        if (statusFile == null) {
            statusFile = new File(reporter.getConfig().getProperty("name", reporter.getName()) + ".status");
        }
        try {
            try (BufferedWriter out = new BufferedWriter(new FileWriter(statusFile))) {
                // 1 jobs, 1 running, 0 reported, 0 failed, 77 pre-existing since 2017-04-01T00:00:00
                // 10 jobs, 0 running, 10 reported, 0 failed, 10 pre-existing since start of service
                out.append(String.valueOf(
                            reportMap.entrySet().size() - preexisting + running.size() + failed.size())).append(
                            " jobs, ").
                            append(String.valueOf(running.size())).append(" running, ").
                            append(String.valueOf(reportMap.entrySet().size() - preexisting)).append(" reported, ").
                            append(String.valueOf(failed.size())).append(" failed, ").
                            append(String.valueOf(preexisting)).append(" pre-existing since ").
                            append(historyDate).append("\n");
                for (String name : failed) {
                    out.append("f ").append(name).append("\n");
                }
                for (String name : running) {
                    out.append("r ").append(name).append("\n");
                }
            }
        } catch (IOException e) {
            LOGGER.warning("failed writing instance log " + reporter.getConfig().getProperty("name",
                                                                                             reporter.getName()) + ".status: " + e.getMessage());
        }
    }
}
