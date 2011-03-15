package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import org.junit.Ignore;

/**
 * A workflow item that runs through the regular life cycle steps UNKNOWN, SCHEDULED, RUNNING and finally COMPLETED.
 *
 * @author MarcoZ
 * @author Norman
 */
@Ignore
public class LifeStepWorkflowItem extends AbstractWorkflowItem {
    private boolean submitted;

    public boolean isSubmitted() {
        return submitted;
    }

    void incLifeStep() {
        if (isSubmitted()) {
            ProcessStatus status = getStatus();
            if (status.equals(ProcessStatus.UNKNOWN)) {
                setStatus(new ProcessStatus(ProcessState.SCHEDULED));
            } else if (status.equals(ProcessStatus.SCHEDULED)) {
                setStatus(new ProcessStatus(ProcessState.RUNNING, 0.5f));
            } else if (status.equals(new ProcessStatus(ProcessState.RUNNING, 0.5f))) {
                setStatus(new ProcessStatus(ProcessState.COMPLETED));
            }
            // Other case are failures: CANCELLED, ERROR
        }
    }

    @Override
    public void submit() {
        submitted = true;
    }

    @Override
    public void kill() throws ProductionException {
    }

    @Override
    public void updateStatus() {
    }

    @Override
    public Object[] getJobIds() {
        return new Object[0];
    }
}
