package com.bc.calvalus.reporting.urban.ws;

/**
 * @author muhammad.bc.
 */
public class WpsReport {
    private String jobID;
    private String accRef;
    private String compID;
    private String status;
    private String hostName;
    private String uri;
    private String finishDateTime;

    public WpsReport(String jobID, String accRef, String compID, String status, String compID1, String uri, String finishDateTime) {
        this.jobID = jobID;
        this.accRef = accRef;
        this.compID = compID;
        this.status = status;
        this.hostName = hostName;
        this.uri = uri;
        this.finishDateTime = finishDateTime;
    }

    public String getFinishDateTime() {
        return finishDateTime;
    }

    public String getJobID() {
        return jobID;
    }

    public String getAccRef() {
        return accRef;
    }

    public String getCompID() {
        return compID;
    }

    public String getStatus() {
        return status;
    }

    public String getHostName() {
        return hostName;
    }

    public String getUri() {
        return uri;
    }
}
