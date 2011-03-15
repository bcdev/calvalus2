package com.bc.calvalus.production;


import com.bc.calvalus.commons.ProcessState;
import org.junit.Test;

import static org.junit.Assert.*;

public class FailedWorkflowTest {

    @Test
    public void testSequential() throws Exception {

        Workflow.Sequential wf = new Workflow.Sequential();
        TestWorkflowItem job1 = new FailingTestWorkflowItem(ProcessState.RUNNING, ProcessState.ERROR);
        TestWorkflowItem job2 = new TestWorkflowItem();

        wf.add(job1, job2);
        wf.submit();
        assertEquals(ProcessState.UNKNOWN, job1.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job2.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, wf.getStatus().getState());

        incLifeStep(job1, job2);
        assertEquals(ProcessState.SCHEDULED, job1.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job2.getStatus().getState());
        assertEquals(ProcessState.SCHEDULED, wf.getStatus().getState());

        incLifeStep(job1, job2);
        assertEquals(ProcessState.RUNNING, job1.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job2.getStatus().getState());
        assertEquals(ProcessState.RUNNING, wf.getStatus().getState());

        incLifeStep(job1, job2);
        assertEquals(ProcessState.ERROR, job1.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job2.getStatus().getState());
        assertEquals(ProcessState.ERROR, wf.getStatus().getState());

        incLifeStep(job1, job2);
        assertEquals(ProcessState.ERROR, job1.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job2.getStatus().getState());
        assertEquals(ProcessState.ERROR, wf.getStatus().getState());
    }

    @Test
    public void testParallel() throws Exception {

        Workflow.Parallel wf = new Workflow.Parallel();

        TestWorkflowItem job1 = new TestWorkflowItem();
        TestWorkflowItem job2 = new FailingTestWorkflowItem(ProcessState.RUNNING, ProcessState.ERROR);

        wf.add(job1, job2);
        wf.submit();
        assertEquals(ProcessState.UNKNOWN, job1.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job2.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, wf.getStatus().getState());

        incLifeStep(job1, job2);
        assertEquals(ProcessState.SCHEDULED, job1.getStatus().getState());
        assertEquals(ProcessState.SCHEDULED, job2.getStatus().getState());
        assertEquals(ProcessState.SCHEDULED, wf.getStatus().getState());

        incLifeStep(job1, job2);
        assertEquals(ProcessState.RUNNING, job1.getStatus().getState());
        assertEquals(ProcessState.RUNNING, job2.getStatus().getState());
        assertEquals(ProcessState.RUNNING, wf.getStatus().getState());

        incLifeStep(job1, job2);
        assertEquals(ProcessState.COMPLETED, job1.getStatus().getState());
        assertEquals(ProcessState.ERROR, job2.getStatus().getState());
        assertEquals(ProcessState.ERROR, wf.getStatus().getState());

        incLifeStep(job1, job2);
        assertEquals(ProcessState.COMPLETED, job1.getStatus().getState());
        assertEquals(ProcessState.ERROR, job2.getStatus().getState());
        assertEquals(ProcessState.ERROR, wf.getStatus().getState());
    }

    @Test
    public void testMixed() throws Exception {

        Workflow.Sequential wf = new Workflow.Sequential();
        TestWorkflowItem job1 = new TestWorkflowItem();
        TestWorkflowItem job2 = new TestWorkflowItem();
        TestWorkflowItem job3 = new FailingTestWorkflowItem(ProcessState.SCHEDULED, ProcessState.ERROR);
        TestWorkflowItem job4 = new TestWorkflowItem();

        Workflow.Parallel wfp = new Workflow.Parallel();
        wfp.add(job1, job2);
        Workflow.Sequential wfs = new Workflow.Sequential();
        wfs.add(job3, job4);
        wf.add(wfp, wfs);

        wf.submit();
        assertEquals(ProcessState.UNKNOWN, job1.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job2.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job3.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job4.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, wfp.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, wfs.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, wf.getStatus().getState());

        incLifeStep(job1, job2, job3, job4);
        assertEquals(ProcessState.SCHEDULED, job1.getStatus().getState());
        assertEquals(ProcessState.SCHEDULED, job2.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job3.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job4.getStatus().getState());
        assertEquals(ProcessState.SCHEDULED, wfp.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, wfs.getStatus().getState());
        assertEquals(ProcessState.SCHEDULED, wf.getStatus().getState());

        incLifeStep(job1, job2, job3, job4);
        assertEquals(ProcessState.RUNNING, job1.getStatus().getState());
        assertEquals(ProcessState.RUNNING, job2.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job3.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job4.getStatus().getState());
        assertEquals(ProcessState.RUNNING, wfp.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, wfs.getStatus().getState());
        assertEquals(ProcessState.RUNNING, wf.getStatus().getState());

        incLifeStep(job1, job2, job3, job4);
        assertEquals(ProcessState.COMPLETED, job1.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, job2.getStatus().getState());
        assertEquals(ProcessState.SCHEDULED, job3.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job4.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, wfp.getStatus().getState());
        assertEquals(ProcessState.SCHEDULED, wfs.getStatus().getState());
        assertEquals(ProcessState.RUNNING, wf.getStatus().getState());

        incLifeStep(job1, job2, job3, job4);
        assertEquals(ProcessState.COMPLETED, job1.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, job2.getStatus().getState());
        assertEquals(ProcessState.ERROR, job3.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job4.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, wfp.getStatus().getState());
        assertEquals(ProcessState.ERROR, wfs.getStatus().getState());
        assertEquals(ProcessState.ERROR, wf.getStatus().getState());

        incLifeStep(job1, job2, job3, job4);
        assertEquals(ProcessState.COMPLETED, job1.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, job2.getStatus().getState());
        assertEquals(ProcessState.ERROR, job3.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job4.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, wfp.getStatus().getState());
        assertEquals(ProcessState.ERROR, wfs.getStatus().getState());
        assertEquals(ProcessState.ERROR, wf.getStatus().getState());
    }

    static void incLifeStep(TestWorkflowItem... jobs) {
        for (TestWorkflowItem job : jobs) {
            job.incLifeStep();
        }
    }

}
