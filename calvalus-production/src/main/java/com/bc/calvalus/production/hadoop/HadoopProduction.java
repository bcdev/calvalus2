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


    public static enum Action {
        NONE,
        CANCEL,
        DELETE,
        RESTART,
    }

    private final JobID jobId;
    private String outputFormat;
    private boolean outputStaging;
    private String wpsXml;
    private boolean hadoopJobDone;
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

    public boolean isHadoopJobDone() {
        return hadoopJobDone;
    }

    public void setHadoopJobDone(boolean hadoopJobDone) {
        this.hadoopJobDone = hadoopJobDone;
    }

    public boolean getOutputStaging() {
        return outputStaging;
    }

    public String getOutputFormat() {
        return outputFormat;
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

    }

    /**
     * Updates the status. This method is called periodically after a fixed delay period.
     * @param jobStatus  The hadoop job status. May be null, which is interpreted as the job is being done.
     */
    public void updateStatus(JobStatus jobStatus) {
        ProductionStatus status = getStatus();
        if (jobStatus == null) {
            hadoopJobDone = true;
            if (getOutputStaging()) {
                status = new ProductionStatus(stagingStatus.getState(), stagingStatus.getMessage(),
                                              (2.0f + stagingStatus.getProgress()) / 3);
            } else {
                status = new ProductionStatus(ProductionState.COMPLETED, 1.0f);
            }
        } else {
            float progress;
            if (getOutputStaging()) {
                progress = (jobStatus.mapProgress() + jobStatus.reduceProgress() + stagingStatus.getProgress()) / 3;
            } else {
                progress = (jobStatus.mapProgress() + jobStatus.reduceProgress()) / 2;
            }
            if (jobStatus.getRunState() == JobStatus.FAILED) {
                hadoopJobDone = true;
                status = new ProductionStatus(ProductionState.ERROR, "Hadoop job '" + jobStatus.getJobID() + "' failed", progress);
            } else if (jobStatus.getRunState() == JobStatus.KILLED) {
                hadoopJobDone = true;
                status = new ProductionStatus(ProductionState.CANCELLED, progress);
            } else if (jobStatus.getRunState() == JobStatus.PREP) {
                status = new ProductionStatus(ProductionState.WAITING, progress);
            } else if (jobStatus.getRunState() == JobStatus.RUNNING) {
                status = new ProductionStatus(ProductionState.IN_PROGRESS, progress);
            } else if (jobStatus.getRunState() == JobStatus.SUCCEEDED) {
                hadoopJobDone = true;
                if (getOutputStaging()) {
                    status = new ProductionStatus(stagingStatus.getState(), stagingStatus.getMessage(), progress);
                } else {
                    status = new ProductionStatus(ProductionState.COMPLETED, 1.0f);
                }
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
