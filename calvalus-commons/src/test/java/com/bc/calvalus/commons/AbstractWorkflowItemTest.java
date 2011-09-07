package com.bc.calvalus.commons;

import org.junit.Before;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.*;

/**
 * @author Norman Fomferra
 */
public class AbstractWorkflowItemTest {

    private AbstractWorkflowItem workflow;

    @Before
    public void setUp() throws Exception {
        workflow = new AbstractWorkflowItem() {
            @Override
            public void kill() throws WorkflowException {
            }

            @Override
            public void submit() throws WorkflowException {
            }

            @Override
            public void updateStatus() {
            }
        };
    }

     @Test
    public void testInitialStatusIsUnknown() {
        assertEquals(ProcessStatus.UNKNOWN, workflow.getStatus());
    }

    @Test(expected = NullPointerException.class)
    public void testStateCannotBeNull() {
        workflow.setStatus(null); // --> NPE expected
    }

    @Test
    public void testTimesAreSetOnStateTransition() {
        assertNull(workflow.getSubmitTime());
        assertNull(workflow.getStartTime());
        assertNull(workflow.getStopTime());

        workflow.setStatus(new ProcessStatus(ProcessState.SCHEDULED));
        assertNotNull(workflow.getSubmitTime());
        assertNull(workflow.getStartTime());
        assertNull(workflow.getStopTime());

        Date submitTime = workflow.getSubmitTime();

        workflow.setStatus(new ProcessStatus(ProcessState.RUNNING));
        assertNotNull(workflow.getSubmitTime());
        assertNotNull(workflow.getStartTime());
        assertNull(workflow.getStopTime());
        assertSame(submitTime, workflow.getSubmitTime());

        Date startTime = workflow.getStartTime();

        workflow.setStatus(new ProcessStatus(ProcessState.RUNNING));
        workflow.setStatus(new ProcessStatus(ProcessState.RUNNING));
        workflow.setStatus(new ProcessStatus(ProcessState.RUNNING));
        assertSame(submitTime, workflow.getSubmitTime());
        assertSame(startTime, workflow.getStartTime());

        workflow.setStatus(new ProcessStatus(ProcessState.COMPLETED));
        assertNotNull(workflow.getSubmitTime());
        assertNotNull(workflow.getStartTime());
        assertNotNull(workflow.getStopTime());

        Date stopTime = workflow.getStopTime();

        workflow.setStatus(new ProcessStatus(ProcessState.UNKNOWN));
        assertNotNull(workflow.getSubmitTime());
        assertNotNull(workflow.getStartTime());
        assertNotNull(workflow.getStopTime());
        assertSame(submitTime, workflow.getSubmitTime());
        assertSame(startTime, workflow.getStartTime());
        assertSame(stopTime, workflow.getStopTime());
    }

}
