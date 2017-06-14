package com.bc.calvalus.reporting.urban;

import com.bc.calvalus.reporting.urban.reporting.CalvalusReport;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
class Report implements Runnable {
    State state = State.NEW;
    String job;
    String creationTime;
    String name;
    String requestId;
    String requestStatus;
    String uri;
    CalvalusReport usageStatistics;
    private UrbanTepReporting reporter;

    public Report(UrbanTepReporting reporter, String job, String creationTime, String name, String requestId, String requestStatus, String uri) {
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

