package com.bc.calvalus.reporting.common;

import com.bc.calvalus.reporting.urban.reporting.CalvalusReport;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class Report implements Runnable {
    public State state = State.NEW;
    public String job;
    public String creationTime;
    public String name;
    public String requestId;
    public String requestStatus;
    public String uri;
    public CalvalusReport usageStatistics;
    private Reporter reporter;

    public Report(Reporter reporter, String job, String creationTime, String name, String requestId, String requestStatus, String uri) {
        this.reporter = reporter;
        this.job = job;
        this.creationTime = creationTime;
        this.name = name;
        this.requestId = requestId;
        this.requestStatus = requestStatus;
        this.uri = uri;
    }

    public void run() {
        reporter.process(this);
    }
}

