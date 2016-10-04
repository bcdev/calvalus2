package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.commons.ProcessState;

import java.util.Date;
import java.util.List;

/**
 * @author hans
 */
public class LocalProductionStatus implements WpsProcessStatus {

    private String jobId;
    private ProcessState state;
    private float progress;
    private String message;
    private List<String> resultUrls;
    private Date stopDate;

    public LocalProductionStatus(String jobId, ProcessState state, float progress, String message, List<String> resultUrls) {
        this.jobId = jobId;
        this.state = state;
        this.progress = progress;
        this.message = message;
        this.resultUrls = resultUrls;
    }

    public String getJobId() {
        return jobId;
    }

    @Override
    public String getState() {
        return state.toString();
    }

    @Override
    public float getProgress() {
        return progress;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public List<String> getResultUrls() {
        return resultUrls;
    }

    @Override
    public Date getStopTime() {
        return stopDate;
    }

    @Override
    public boolean isDone() {
        return state.isDone();
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public void setState(ProcessState state) {
        this.state = state;
    }

    public void setProgress(float progress) {
        this.progress = progress;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setResultUrls(List<String> resultUrls) {
        this.resultUrls = resultUrls;
    }

    public void setStopDate(Date stopDate) {
        this.stopDate = stopDate;
    }
}
