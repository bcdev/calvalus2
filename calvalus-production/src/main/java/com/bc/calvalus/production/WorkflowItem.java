package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessStatus;

/**
 * A workflow item.
 *
 * @author MarcoZ
 * @author Norman
 */
public interface WorkflowItem {
    void submit();

    ProcessStatus getStatus();

    void addStateChangeListener(StateChangeListener listener);

    /**
     * A listener for state changes in workflow items.
     */
    public interface StateChangeListener {
        void handleStateChanged(WorkflowItem item);
    }
}
