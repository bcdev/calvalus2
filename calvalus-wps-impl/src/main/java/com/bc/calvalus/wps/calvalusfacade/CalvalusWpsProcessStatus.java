package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.wps.localprocess.WpsProcessStatus;

import java.util.Date;
import java.util.List;

/**
 * @author hans
 */
public class CalvalusWpsProcessStatus implements WpsProcessStatus {

    private String jobid;
    private String state;
    private float progress;
    private String message;
    private List<String> resultUrls;
    private Date stopTime;
    private boolean done;

    public CalvalusWpsProcessStatus(Production production, List<String> productResultUrls) {
        this.jobid = production.getId();
        this.state = production.getProcessingStatus().getState().toString();
        this.progress = production.getProcessingStatus().getProgress();
        this.message = production.getProcessingStatus().getMessage();
        this.resultUrls = productResultUrls;
        this.stopTime = production.getWorkflow().getStopTime();
        this.done = production.getProcessingStatus().getState().isDone();
    }

    @Override
    public String getJobId() {
        return jobid;
    }

    @Override
    public String getState() {
        return state;
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
        return stopTime;
    }

    @Override
    public boolean isDone() {
        return done;
    }
}
