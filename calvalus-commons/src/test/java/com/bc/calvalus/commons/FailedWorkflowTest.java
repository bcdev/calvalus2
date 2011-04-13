package com.bc.calvalus.commons;


import org.junit.Test;

import static org.junit.Assert.*;

public class FailedWorkflowTest {

    @Test
    public void testSequential() throws Exception {

        Workflow.Sequential wf = new Workflow.Sequential();
        LifeStepWorkflowItem job1 = new FailingLifeStepWorkflowItem(ProcessState.RUNNING, ProcessState.ERROR);
        LifeStepWorkflowItem job2 = new LifeStepWorkflowItem();

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

        LifeStepWorkflowItem job1 = new LifeStepWorkflowItem();
        LifeStepWorkflowItem job2 = new FailingLifeStepWorkflowItem(ProcessState.RUNNING, ProcessState.ERROR);

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
        LifeStepWorkflowItem job1 = new LifeStepWorkflowItem();
        LifeStepWorkflowItem job2 = new LifeStepWorkflowItem();
        LifeStepWorkflowItem job3 = new FailingLifeStepWorkflowItem(ProcessState.SCHEDULED, ProcessState.ERROR);
        LifeStepWorkflowItem job4 = new LifeStepWorkflowItem();

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

    static void incLifeStep(LifeStepWorkflowItem... jobs) {
        for (LifeStepWorkflowItem job : jobs) {
            job.incLifeStep();
        }
    }

}
