package com.bc.calvalus.wps.localprocess;

import java.util.List;

/**
 * @author hans
 */
public class ProductionStatus {

    private String jobId;
    private ProductionState state;
    private float progress;
    private String message;
    private List<String> resultUrls;

    public ProductionStatus(String jobId, ProductionState state, float progress, String message, List<String> resultUrls) {
        this.jobId = jobId;
        this.state = state;
        this.progress = progress;
        this.message = message;
        this.resultUrls = resultUrls;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public ProductionState getState() {
        return state;
    }

    public void setState(ProductionState state) {
        this.state = state;
    }

    public float getProgress() {
        return progress;
    }

    public void setProgress(float progress) {
        this.progress = progress;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<String> getResultUrls() {
        return resultUrls;
    }

    public void setResultUrls(List<String> resultUrls) {
        this.resultUrls = resultUrls;
    }
}
