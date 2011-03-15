package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessStatus;

/**
 * A workflow item. Clients should not implement this interface directly. Instead, they should
 * use {@link AbstractWorkflowItem} as base class.
 *
 * @author MarcoZ
 * @author Norman
 */
public interface WorkflowItem {
    /**
     * Submits this work item's job to the underlying job engine.
     * Usually, implementations of this method should return immediately after the job has been submitted.
     * In rare cases the method blocks, e.g. until the job has completed.
     */
    void submit();

    /**
     * Kills this work item's job in the underlying job engine.
     * Usually, implementations of this method should return immediately after the kill request has been submitted.
     * In rare cases the method blocks, e.g. until the job has been terminated.
     */
    void kill();

    ProcessStatus getStatus();

    void setStatus(ProcessStatus status);

    void addStateChangeListener(StateChangeListener listener);

    /**
     * A listener for state changes in workflow items.
     */
    public interface StateChangeListener {
        void handleStateChanged(WorkflowItem item);
    }
}
