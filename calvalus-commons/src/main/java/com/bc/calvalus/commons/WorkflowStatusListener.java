package com.bc.calvalus.commons;

/**
 * A listener for status changes in workflow items.
 */
public interface WorkflowStatusListener {
    /**
     * Called if the status of a workflow item's process has changed.
     *
     * @param event The workflow status event.
     */
    void handleStatusChanged(WorkflowStatusEvent event);
}
