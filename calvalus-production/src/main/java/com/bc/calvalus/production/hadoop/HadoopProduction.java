package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionState;
import com.bc.calvalus.production.ProductionStatus;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.JobID;

/**
 * A Hadoop workflow.
 *
 * @author Norman
 */
class HadoopProduction extends Production {

    private String wpsXml;

    public static enum Action {
        NONE,
        CANCEL,
        DELETE,
        RESTART,
    }

    private final JobID jobId;
    private String outputFormat;
    private boolean outputStaging;

    private JobStatus jobStatus;
    private ProductionStatus stagingStatus;

    private Action action;

    public HadoopProduction(String id,
                            String name,
                            JobID jobId,
                            String outputPath,
                            String outputFormat,
                            boolean outputStaging) {
        super(id, name, outputPath);
        this.jobId = jobId;
        this.outputFormat = outputFormat;
        this.outputStaging = outputStaging;
        this.action = Action.NONE;
        this.stagingStatus = new ProductionStatus();
    }

    public JobID getJobId() {
        return jobId;
    }

    public boolean getOutputStaging() {
        return outputStaging;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public JobStatus getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
        updateStatus();
    }

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    public ProductionStatus getStagingStatus() {
        return stagingStatus;
    }

    public void setStagingStatus(ProductionStatus stagingStatus) {
        this.stagingStatus = stagingStatus;
        updateStatus();
    }

    private void updateStatus() {
        ProductionStatus status = getStatus();


        // todo - use message that shows current 'action' value from 'HadoopProduction'
        float progress;
        if (getOutputStaging()) {
            progress = (jobStatus.mapProgress() + jobStatus.reduceProgress() + stagingStatus.getProgress()) / 3;
        } else {
            progress = (jobStatus.mapProgress() + jobStatus.reduceProgress()) / 2;
        }
        if (jobStatus.getRunState() == JobStatus.FAILED) {
            status = new ProductionStatus(ProductionState.ERROR, progress);
        } else if (jobStatus.getRunState() == JobStatus.KILLED) {
            status = new ProductionStatus(ProductionState.CANCELLED, progress);
        } else if (jobStatus.getRunState() == JobStatus.PREP) {
            status = new ProductionStatus(ProductionState.WAITING, progress);
        } else if (jobStatus.getRunState() == JobStatus.RUNNING) {
            status = new ProductionStatus(ProductionState.IN_PROGRESS, progress);
        } else if (jobStatus.getRunState() == JobStatus.SUCCEEDED) {
            if (getOutputStaging()) {
                status = new ProductionStatus(stagingStatus.getState(), stagingStatus.getMessage(), progress);
            } else {
                status = new ProductionStatus(ProductionState.COMPLETED, 1.0f);
            }
        }

        setStatus(status);
    }

    String getWpsXml() {
        return wpsXml;
    }

    void setWpsXml(String wpsXml) {
        //To change body of created methods use File | Settings | File Templates.
        this.wpsXml = wpsXml;
    }

}
