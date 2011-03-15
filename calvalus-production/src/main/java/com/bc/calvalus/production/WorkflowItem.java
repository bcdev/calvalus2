package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessStatus;

/**
 * A workflow item. Clients should not implement this interface directly. Instead, they should
 * use {@link AbstractWorkflowItem} as base class.
 *
 * @author MarcoZ
 * @author Norman
 */
public interface WorkflowItem<JOB_ID> {
    /**
     * @return The (job) identifier, or {@code null}. It is {@code null} either if the job has not been submitted or if the item does
     *         not support identifiers.
     */
//    JOB_ID getId();

    /**
     * Submits this work item's job to the underlying job engine.
     * Usually, implementations of this method should return immediately after the job has been submitted.
     * In rare cases the method blocks, e.g. until the job has completed.
     *
     * @throws ProductionException If the submission fails.
     */
    void submit() throws ProductionException;

    /**
     * Kills this work item's job in the underlying job engine.
     * Usually, implementations of this method should return immediately after the kill request has been submitted.
     * In rare cases the method blocks, e.g. until the job has been terminated.
     *
     * @throws ProductionException If the kill request fails.
     */
    void kill() throws ProductionException;

    /**
     * Asks this workflow item to update its job status from the underlying job engine.
     */
    void updateStatus();

    ProcessStatus getStatus();

    void setStatus(ProcessStatus status);

    void addStateChangeListener(StateChangeListener listener);

    @Deprecated
    Object[] getJobIds();

    /**
     * A listener for state changes in workflow items.
     */
    public interface StateChangeListener {
        void handleStateChanged(WorkflowItem item);
    }
}
