package com.bc.calvalus.processing.hadoop;

import com.bc.calvalus.commons.AbstractWorkflowItem;
import com.bc.calvalus.commons.ProcessStatus;
import org.apache.hadoop.mapreduce.JobID;

import java.io.IOException;

/**
 * A workflow item that corresponds to a single Hadoop job.
 */
public abstract class HadoopWorkflowItem extends AbstractWorkflowItem {
    private final HadoopProcessingService processingService;
    private JobID jobId;

    public HadoopWorkflowItem(HadoopProcessingService processingService) {
        this.processingService = processingService;
    }

    public HadoopProcessingService getProcessingService() {
        return processingService;
    }

    public JobID getJobId() {
        return jobId;
    }

    public void setJobId(JobID jobId) {
        this.jobId = jobId;
    }

    @Override
     public void kill() throws Exception {
         try {
             processingService.killJob(jobId);
         } catch (IOException e) {
             throw new Exception("Failed to kill Hadoop job: " + e.getMessage(), e);
         }
     }

     @Override
     public void updateStatus() {
         if (jobId != null) {
             ProcessStatus jobStatus = processingService.getJobStatus(jobId);
             if (jobStatus != null) {
                 setStatus(jobStatus);
             }
         }
     }

     @Override
     public Object[] getJobIds() {
         return jobId != null ? new Object[]{jobId} : new Object[0];
     }
}
