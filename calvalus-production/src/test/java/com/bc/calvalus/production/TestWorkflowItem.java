package com.bc.calvalus.production;

import org.junit.Ignore;

/**
 * A simple WorkflowItem with an ID but that does nothing on submit(), kill() and updateStatus().
 */
@Ignore
public class TestWorkflowItem<T> extends AbstractWorkflowItem {
    private final T jobId;

    TestWorkflowItem(T jobId) {
        this.jobId = jobId;
    }

    public T getJobId() {
        return jobId;
    }

    @Override
    public void submit() throws ProductionException {
    }

    @Override
    public void kill() throws ProductionException {
    }

    @Override
    public void updateStatus() {
    }

    @Override
    public Object[] getJobIds() {
        return new Object[]{jobId};
    }
}
