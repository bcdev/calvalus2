package com.bc.calvalus.wps.localprocess;

import java.util.Map;

/**
 * @author hans
 */
public class LocalJob {

    private String jobid;
    private Map<String, Object> parameters;
    private LocalProductionStatus status;

    public LocalJob(String jobid, Map<String, Object> parameters, LocalProductionStatus status) {
        this.jobid = jobid;
        this.parameters = parameters;
        this.status = status;
    }

    public String getId() {
        return jobid;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public LocalProductionStatus getStatus() {
        return status;
    }

    public void updateStatus(LocalProductionStatus newStatus) {
        this.status = newStatus;
    }
}
