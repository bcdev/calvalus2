package com.bc.calvalus.commons;


import org.junit.Test;

import static com.bc.calvalus.commons.ProcessState.*;
import static org.junit.Assert.*;

public class FailedWorkflowTest {


    @Test
    public void testSequential() throws Exception {

        Workflow.Sequential wf = new Workflow.Sequential();
        LifeStepWorkflowItem job1 = new FailingLifeStepWorkflowItem(RUNNING, ERROR);
        LifeStepWorkflowItem job2 = new LifeStepWorkflowItem();

        wf.add(job1, job2);
        wf.submit();
        assertState(UNKNOWN, job1);
        assertState(UNKNOWN, job2);
        assertState(UNKNOWN, wf);

        incLifeStep(job1, job2);
        assertState(SCHEDULED, job1);
        assertState(UNKNOWN, job2);
        assertState(SCHEDULED, wf);

        incLifeStep(job1, job2);
        assertState(RUNNING, job1);
        assertState(UNKNOWN, job2);
        assertState(RUNNING, wf);

        incLifeStep(job1, job2);
        assertState(ERROR, job1);
        assertState(ERROR, job2);
        assertState(ERROR, wf);

        incLifeStep(job1, job2);
        assertState(ERROR, job1);
        assertState(ERROR, job2);
        assertState(ERROR, wf);
    }

    @Test
    public void testSequential_NotSustainable() throws Exception {

        Workflow.Sequential wf = new Workflow.Sequential();
        wf.setSustainable(false);
        LifeStepWorkflowItem job1 = new FailingLifeStepWorkflowItem(RUNNING, ERROR);
        LifeStepWorkflowItem job2 = new LifeStepWorkflowItem();

        wf.add(job1, job2);
        wf.submit();
        assertState(UNKNOWN, job1);
        assertState(UNKNOWN, job2);
        assertState(UNKNOWN, wf);

        incLifeStep(job1, job2);
        assertState(SCHEDULED, job1);
        assertState(UNKNOWN, job2);
        assertState(SCHEDULED, wf);

        incLifeStep(job1, job2);
        assertState(RUNNING, job1);
        assertState(UNKNOWN, job2);
        assertState(RUNNING, wf);

        incLifeStep(job1, job2);
        assertState(ERROR, job1);
        assertState(SCHEDULED, job2);
        assertState(SCHEDULED, wf);

        incLifeStep(job1, job2);
        assertState(ERROR, job1);
        assertState(RUNNING, job2);
        assertState(RUNNING, wf);

        incLifeStep(job1, job2);
        assertState(ERROR, job1);
        assertState(COMPLETED, job2);
        assertState(COMPLETED, wf);

    }

    /**
     * Test that a single failing job results in an aggregated error state.
     */
    @Test
    public void testParallel_ErrorState() throws Exception {

        Workflow.Parallel wf = new Workflow.Parallel();

        LifeStepWorkflowItem job1 = new LifeStepWorkflowItem();
        LifeStepWorkflowItem job2 = new FailingLifeStepWorkflowItem(RUNNING, ERROR);

        wf.add(job1, job2);
        wf.submit();
        assertState(UNKNOWN, job1);
        assertState(UNKNOWN, job2);
        assertState(UNKNOWN, wf);

        incLifeStep(job1, job2);
        assertState(SCHEDULED, job1);
        assertState(SCHEDULED, job2);
        assertState(SCHEDULED, wf);

        incLifeStep(job1, job2);
        assertState(RUNNING, job1);
        assertState(RUNNING, job2);
        assertState(RUNNING, wf);

        incLifeStep(job1, job2);
        assertState(COMPLETED, job1);
        assertState(ERROR, job2);
        assertState(ERROR, wf);

        incLifeStep(job1, job2);
        assertState(COMPLETED, job1);
        assertState(ERROR, job2);
        assertState(ERROR, wf);
    }

    @Test
    public void testParallel_WithFailing_Sustainable() throws Exception {

        Workflow.Parallel wf = new Workflow.Parallel();

        LifeStepWorkflowItem job1 = new FailingLifeStepWorkflowItem(RUNNING, ERROR);
        LifeStepWorkflowItem job2 = new LifeStepWorkflowItem(2);

        wf.add(job1, job2);
        wf.submit();
        assertState(UNKNOWN, job1);
        assertState(UNKNOWN, job2);
        assertState(UNKNOWN, wf);

        incLifeStep(job1, job2);
        assertState(SCHEDULED, job1);
        assertState(SCHEDULED, job2);
        assertState(SCHEDULED, wf);

        incLifeStep(job1, job2);
        assertState(RUNNING, job1);
        assertState(RUNNING, job2);
        assertState(RUNNING, wf);

        incLifeStep(job1, job2);
        assertState(ERROR, job1);
        assertState(ERROR, job2);
        assertState(ERROR, wf);

        incLifeStep(job1, job2);
        assertState(ERROR, job1);
        assertState(ERROR, job2);
        assertState(ERROR, wf);
    }

    @Test
    public void testParallel_WithFailing_Unsustainable() throws Exception {

        Workflow.Parallel wf = new Workflow.Parallel();
        wf.setSustainable(false);

        LifeStepWorkflowItem job1 = new FailingLifeStepWorkflowItem(RUNNING, ERROR);
        LifeStepWorkflowItem job2 = new LifeStepWorkflowItem(2);

        wf.add(job1, job2);
        wf.submit();
        assertState(UNKNOWN, job1);
        assertState(UNKNOWN, job2);
        assertState(UNKNOWN, wf);

        incLifeStep(job1, job2);
        assertState(SCHEDULED, job1);
        assertState(SCHEDULED, job2);
        assertState(SCHEDULED, wf);

        incLifeStep(job1, job2);
        assertState(RUNNING, job1);
        assertState(RUNNING, job2);
        assertState(RUNNING, wf);

        incLifeStep(job1, job2);
        assertState(ERROR, job1);
        assertState(RUNNING, job2);
        assertState(RUNNING, wf);

        incLifeStep(job1, job2);
        assertState(ERROR, job1);
        assertState(COMPLETED, job2);
        assertState(COMPLETED, wf);

        incLifeStep(job1, job2);
        assertState(ERROR, job1);
        assertState(COMPLETED, job2);
        assertState(COMPLETED, wf);
    }

    @Test
    public void testParallel_ParallelOfSerials_Sustainable() throws Exception {

        Workflow.Parallel parallel = new Workflow.Parallel();

        Workflow.Sequential seq1 = new Workflow.Sequential();
        LifeStepWorkflowItem job1 = new LifeStepWorkflowItem(2);
        LifeStepWorkflowItem job2 = new LifeStepWorkflowItem(1);
        seq1.add(job1, job2);

        Workflow.Sequential seq2 = new Workflow.Sequential();
        LifeStepWorkflowItem job3 = new FailingLifeStepWorkflowItem(RUNNING, ERROR);
        LifeStepWorkflowItem job4 = new LifeStepWorkflowItem(1);
        seq2.add(job3, job4);

        parallel.add(seq1, seq2);
        parallel.submit();
        assertState(UNKNOWN, job1);
        assertState(UNKNOWN, job2);
        assertState(UNKNOWN, job3);
        assertState(UNKNOWN, job4);
        assertState(UNKNOWN, seq1);
        assertState(UNKNOWN, seq2);
        assertState(UNKNOWN, parallel);

        incLifeStep(job1, job2, job3, job4);
        assertState(SCHEDULED, job1);
        assertState(UNKNOWN, job2);
        assertState(SCHEDULED, job3);
        assertState(UNKNOWN, job4);
        assertState(SCHEDULED, seq1);
        assertState(SCHEDULED, seq2);
        assertState(SCHEDULED, parallel);

        incLifeStep(job1, job2, job3, job4);
        assertState(RUNNING, job1);
        assertState(UNKNOWN, job2);
        assertState(RUNNING, job3);
        assertState(UNKNOWN, job4);
        assertState(RUNNING, seq1);
        assertState(RUNNING, seq2);
        assertState(RUNNING, parallel);

        incLifeStep(job1, job2, job3, job4);
        assertState(ERROR, job1);
        assertState(ERROR, job2);
        assertState(ERROR, job3);
        assertState(ERROR, job4);
        assertState(ERROR, seq1);
        assertState(ERROR, seq2);
        assertState(ERROR, parallel);

        incLifeStep(job1, job2, job3, job4);
        assertState(ERROR, job1);
        assertState(ERROR, job2);
        assertState(ERROR, job3);
        assertState(ERROR, job4);
        assertState(ERROR, seq1);
        assertState(ERROR, seq2);
        assertState(ERROR, parallel);
    }

    @Test
    public void testParallel_ParallelOfSerials_Unsustainable() throws Exception {

        Workflow.Parallel parallel = new Workflow.Parallel();
        parallel.setSustainable(false);

        Workflow.Sequential seq1 = new Workflow.Sequential();
        LifeStepWorkflowItem job1 = new LifeStepWorkflowItem(2);
        LifeStepWorkflowItem job2 = new LifeStepWorkflowItem(1);
        seq1.add(job1, job2);

        Workflow.Sequential seq2 = new Workflow.Sequential();
        LifeStepWorkflowItem job3 = new FailingLifeStepWorkflowItem(RUNNING, ERROR);
        LifeStepWorkflowItem job4 = new LifeStepWorkflowItem(1);
        seq2.add(job3, job4);

        parallel.add(seq1, seq2);
        parallel.submit();
        assertState(UNKNOWN, job1);
        assertState(UNKNOWN, job2);
        assertState(UNKNOWN, job3);
        assertState(UNKNOWN, job4);
        assertState(UNKNOWN, seq1);
        assertState(UNKNOWN, seq2);
        assertState(UNKNOWN, parallel);

        incLifeStep(job1, job2, job3, job4);
        assertState(SCHEDULED, job1);
        assertState(UNKNOWN, job2);
        assertState(SCHEDULED, job3);
        assertState(UNKNOWN, job4);
        assertState(SCHEDULED, seq1);
        assertState(SCHEDULED, seq2);
        assertState(SCHEDULED, parallel);

        incLifeStep(job1, job2, job3, job4);
        assertState(RUNNING, job1);
        assertState(UNKNOWN, job2);
        assertState(RUNNING, job3);
        assertState(UNKNOWN, job4);
        assertState(RUNNING, seq1);
        assertState(RUNNING, seq2);
        assertState(RUNNING, parallel);

        incLifeStep(job1, job2, job3, job4);
        assertState(RUNNING, job1);
        assertState(UNKNOWN, job2);
        assertState(ERROR, job3);
        assertState(ERROR, job4);
        assertState(RUNNING, seq1);
        assertState(ERROR, seq2);
        assertState(RUNNING, parallel);

        incLifeStep(job1, job2, job3, job4);
        assertState(COMPLETED, job1);
        assertState(SCHEDULED, job2);
        assertState(ERROR, job3);
        assertState(ERROR, job4);
        assertState(RUNNING, seq1);
        assertState(ERROR, seq2);
        assertState(RUNNING, parallel);

        incLifeStep(job1, job2, job3, job4);
        assertState(COMPLETED, job1);
        assertState(RUNNING, job2);
        assertState(ERROR, job3);
        assertState(ERROR, job4);
        assertState(RUNNING, seq1);
        assertState(ERROR, seq2);
        assertState(RUNNING, parallel);

        incLifeStep(job1, job2, job3, job4);
        assertState(COMPLETED, job1);
        assertState(COMPLETED, job2);
        assertState(ERROR, job3);
        assertState(ERROR, job4);
        assertState(COMPLETED, seq1);
        assertState(ERROR, seq2);
        assertState(COMPLETED, parallel);

        incLifeStep(job1, job2, job3, job4);
        assertState(COMPLETED, job1);
        assertState(COMPLETED, job2);
        assertState(ERROR, job3);
        assertState(ERROR, job4);
        assertState(COMPLETED, seq1);
        assertState(ERROR, seq2);
        assertState(COMPLETED, parallel);

    }

    @Test
    public void testMixed() throws Exception {

        Workflow.Sequential wf = new Workflow.Sequential();
        LifeStepWorkflowItem job1 = new LifeStepWorkflowItem();
        LifeStepWorkflowItem job2 = new LifeStepWorkflowItem();
        LifeStepWorkflowItem job3 = new FailingLifeStepWorkflowItem(SCHEDULED, ERROR);
        LifeStepWorkflowItem job4 = new LifeStepWorkflowItem();

        Workflow.Parallel wfp = new Workflow.Parallel();
        wfp.add(job1, job2);
        Workflow.Sequential wfs = new Workflow.Sequential();
        wfs.add(job3, job4);
        wf.add(wfp, wfs);

        wf.submit();
        assertState(UNKNOWN, job1);
        assertState(UNKNOWN, job2);
        assertState(UNKNOWN, job3);
        assertState(UNKNOWN, job4);
        assertState(UNKNOWN, wfp);
        assertState(UNKNOWN, wfs);
        assertState(UNKNOWN, wf);

        incLifeStep(job1, job2, job3, job4);
        assertState(SCHEDULED, job1);
        assertState(SCHEDULED, job2);
        assertState(UNKNOWN, job3);
        assertState(UNKNOWN, job4);
        assertState(SCHEDULED, wfp);
        assertState(UNKNOWN, wfs);
        assertState(SCHEDULED, wf);

        incLifeStep(job1, job2, job3, job4);
        assertState(RUNNING, job1);
        assertState(RUNNING, job2);
        assertState(UNKNOWN, job3);
        assertState(UNKNOWN, job4);
        assertState(RUNNING, wfp);
        assertState(UNKNOWN, wfs);
        assertState(RUNNING, wf);

        incLifeStep(job1, job2, job3, job4);
        assertState(COMPLETED, job1);
        assertState(COMPLETED, job2);
        assertState(SCHEDULED, job3);
        assertState(UNKNOWN, job4);
        assertState(COMPLETED, wfp);
        assertState(SCHEDULED, wfs);
        assertState(RUNNING, wf);

        incLifeStep(job1, job2, job3, job4);
        assertState(COMPLETED, job1);
        assertState(COMPLETED, job2);
        assertState(ERROR, job3);
        assertState(ERROR, job4);
        assertState(COMPLETED, wfp);
        assertState(ERROR, wfs);
        assertState(ERROR, wf);

        incLifeStep(job1, job2, job3, job4);
        assertState(COMPLETED, job1);
        assertState(COMPLETED, job2);
        assertState(ERROR, job3);
        assertState(ERROR, job4);
        assertState(COMPLETED, wfp);
        assertState(ERROR, wfs);
        assertState(ERROR, wf);
    }

    static void assertState(ProcessState expected, WorkflowItem actualWFI) {
        assertEquals(expected, actualWFI.getStatus().getState());
    }

    static void incLifeStep(LifeStepWorkflowItem... jobs) {
        for (LifeStepWorkflowItem job : jobs) {
            job.incLifeStep();
        }
    }

}
