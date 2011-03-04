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
    private final JobID jobId;
    private final boolean outputStaging;
    private final ProductionRequest productionRequest;

    // variable
    private Action action;

    public HadoopProduction(String id,
                            String name,
                            JobID jobId,
                            boolean outputStaging,
                            ProductionRequest productionRequest) {
        super(id, name);
        this.jobId = jobId;
        this.productionRequest = productionRequest;
        this.outputStaging = outputStaging;
        this.action = Action.NONE;
        if (outputStaging) {
            setStagingStatus(new ProductionStatus(ProductionState.WAITING));
        }
    }

    public JobID getJobId() {
        return jobId;
    }

    public ProductionRequest getProductionRequest() {
        return productionRequest;
    }

    public boolean isOutputStaging() {
        return outputStaging;
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
