package com.bc.calvalus.commons;

import java.util.EventObject;

/**
* A workflow status change event.
*/
public class WorkflowStatusEvent extends EventObject {
    private final ProcessStatus oldStatus;
    private final ProcessStatus newStatus;

    public WorkflowStatusEvent(WorkflowItem source, ProcessStatus oldStatus, ProcessStatus newStatus) {
        super(source);
        this.oldStatus = oldStatus;
        this.newStatus = newStatus;
    }

    @Override
    public WorkflowItem getSource() {
        return (WorkflowItem) super.getSource();
    }

    public ProcessStatus getOldStatus() {
        return oldStatus;
    }

    public ProcessStatus getNewStatus() {
        return newStatus;
    }
}
