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
    private JobStatus jobStatus;
    private Action action;
    private boolean stagingAfterProduction;

    ProductionState stagingState;

    public HadoopProduction(String id, String name, String outputPath, JobID jobId) {
        super(id, name, outputPath);
        this.jobId = jobId;
        this.action = Action.NONE;
        this.stagingState = stagingAfterProduction ? ProductionState.WAITING : ProductionState.UNKNOWN;
    }

    public JobID getJobId() {
        return jobId;
    }

    public JobStatus getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
        setStatus(getProductionStatus(jobStatus));
    }

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    public boolean isStagingAfterProduction() {
        return stagingAfterProduction;
    }

    public void setStagingAfterProduction(boolean stagingAfterProduction) {
        this.stagingAfterProduction = stagingAfterProduction;
    }

    public ProductionState getStagingState() {
        return stagingState;
    }

    public void setStagingState(ProductionState stagingState) {
        this.stagingState = stagingState;
    }

    public static ProductionStatus getProductionStatus(JobStatus job) {
        if (job != null) {
            // todo - use message that shows current 'action' value from 'HadoopProduction'
            float progress = 0.5f * (job.mapProgress() + job.reduceProgress());
            if (job.getRunState() == JobStatus.FAILED) {
                return new ProductionStatus(ProductionState.ERROR, progress);
            } else if (job.getRunState() == JobStatus.KILLED) {
                return new ProductionStatus(ProductionState.CANCELLED, progress);
            } else if (job.getRunState() == JobStatus.PREP) {
                return new ProductionStatus(ProductionState.WAITING, progress);
            } else if (job.getRunState() == JobStatus.RUNNING) {
                return new ProductionStatus(ProductionState.IN_PROGRESS, progress);
            } else if (job.getRunState() == JobStatus.SUCCEEDED) {

                return new ProductionStatus(ProductionState.COMPLETED);
            }
        }
        return new ProductionStatus(ProductionState.UNKNOWN);
    }
}
