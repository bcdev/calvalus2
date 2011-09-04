package com.bc.calvalus.production.store;

import com.bc.calvalus.commons.AbstractWorkflowItem;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowException;
import com.bc.calvalus.processing.ProcessingService;

import java.util.Date;

/**
 * Used as a proxy for a 'real' workflow so that status information can be retrieved from the processing service.
 * They are created if a production is deserialised from a database.
 * <p/>
 * {@code ProxyWorkflow}s can neither be submitted nor killed.
 * The only methods that can be called are {@link #updateStatus()} and {@link #getStatus()},
 * all other methods will have no effect.
 *
 * @author Norman Fomferra
 */
class ProxyWorkflow extends AbstractWorkflowItem {
    private final ProcessingService processingService;
    private final Object[] jobIds;

    public ProxyWorkflow(ProcessingService processingService,
                         Object[] jobIds,
                         Date submitTime,
                         Date startTime,
                         Date stopTime,
                         ProcessStatus status) {
        this.processingService = processingService;
        this.jobIds = jobIds;
        setSubmitTime(submitTime);
        setStartTime(startTime);
        setStopTime(stopTime);
        setStatus(status);
    }

    @Override
    public void submit() throws WorkflowException {
    }

    @Override
    public void kill() throws WorkflowException {
    }

    @Override
    public void updateStatus() {
        ProcessStatus[] processStatuses = new ProcessStatus[jobIds.length];
        for (int i = 0; i < jobIds.length; i++) {
            Object jobId = jobIds[i];
            processStatuses[i] = processingService.getJobStatus(jobId);
        }
        setStatus(ProcessStatus.aggregate(processStatuses));
    }

    @Override
    public Object[] getJobIds() {
        return jobIds;
    }

}
