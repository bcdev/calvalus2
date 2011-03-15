package com.bc.calvalus.commons;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for workflow item implementations.
 * Clients only must implement {@link #submit()}.
 *
 * @author Norman
 */
public abstract class AbstractWorkflowItem implements WorkflowItem {
    private final List<StateChangeListener> changeListeners;
    private ProcessStatus status;

    public AbstractWorkflowItem() {
        this.changeListeners = new ArrayList<StateChangeListener>();
        this.status = ProcessStatus.UNKNOWN;
    }

    @Override
    public void addStateChangeListener(StateChangeListener l) {
        changeListeners.add(l);
    }

    @Override
    public ProcessStatus getStatus() {
        return status;
    }

    @Override
    public void setStatus(ProcessStatus status) {
        ProcessState oldState = this.status.getState();
        this.status = status;
        if (oldState != status.getState()) {
            // todo: do not allow transition to unknown state
            fireStateChanged();
        }
    }

    protected void fireStateChanged() {
        for (StateChangeListener changeListener : changeListeners) {
            changeListener.handleStateChanged(this);
        }
    }
}
