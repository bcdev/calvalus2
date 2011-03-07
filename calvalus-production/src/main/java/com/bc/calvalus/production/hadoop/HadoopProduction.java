package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionRequest;
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
        RESTART,// todo - implement restart
    }

    // final
    private final JobID[]  jobIds;

    // variable
    private Action action;
    private StagingJob stagingJob;

    public HadoopProduction(String id,
                            String name,
                            boolean outputStaging, JobID[] jobIds,
                            ProductionRequest productionRequest) {
        super(id, name, outputStaging, productionRequest);
        this.jobIds = jobIds.clone();
        this.action = Action.NONE;
        if (outputStaging) {
            setStagingStatus(new ProductionStatus(ProductionState.WAITING));
        }
    }

    public JobID getJobId() {
        return jobIds[0];
    }

    public JobID[] getJobIds() {
        return jobIds.clone();
    }

    public StagingJob getStagingJob() {
        return stagingJob;
    }

    public void setStagingJob(StagingJob stagingJob) {
        this.stagingJob = stagingJob;
    }


    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    /**
     * Updates the status. This method is called periodically after a fixed delay period.
     *
     * @param jobStatus The hadoop job status. May be null, which is interpreted as the job is being done.
     */
    public void setProductionStatus(JobStatus jobStatus) {
        if (jobStatus != null) {
            ProductionStatus status = getProcessingStatus();
            float progress = (jobStatus.mapProgress() + jobStatus.reduceProgress()) / 2;
            if (jobStatus.getRunState() == JobStatus.FAILED) {
                status = new ProductionStatus(ProductionState.ERROR, progress, "Hadoop job '" + jobStatus.getJobID() + "' failed");
            } else if (jobStatus.getRunState() == JobStatus.KILLED) {
                status = new ProductionStatus(ProductionState.CANCELLED, progress);
            } else if (jobStatus.getRunState() == JobStatus.PREP) {
                status = new ProductionStatus(ProductionState.WAITING, progress);
            } else if (jobStatus.getRunState() == JobStatus.RUNNING) {
                status = new ProductionStatus(ProductionState.IN_PROGRESS, progress);
            } else if (jobStatus.getRunState() == JobStatus.SUCCEEDED) {
                status = new ProductionStatus(ProductionState.COMPLETED, 1.0f);
            }
            setProcessingStatus(status);
        }
    }
}
