package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import org.junit.Ignore;

/**
 * A workflow item that runs through the regular life cycle steps UNKNOWN, SCHEDULED, RUNNING and finally COMPLETED.
 *
 * @author Norman
 */
@Ignore
public class FailingLifeStepWorkflowItem extends LifeStepWorkflowItem {
    private final ProcessState stateBeforeFailure;
    private final ProcessState failureState;

    public FailingLifeStepWorkflowItem(ProcessState stateBeforeFailure, ProcessState failureState) {
        this.stateBeforeFailure = stateBeforeFailure;
        this.failureState = failureState;
    }

    @Override
    void incLifeStep() {
        if (isSubmitted()) {
            ProcessStatus status = getStatus();
            if (status.getState() == stateBeforeFailure) {
                setStatus(new ProcessStatus(failureState));
            }else {
                super.incLifeStep();
            }
        }
    }

}
