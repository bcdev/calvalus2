package com.bc.calvalus.production;


import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import org.junit.Test;

import static org.junit.Assert.*;

public class WorkflowTest {

    @Test
    public void testSequential() throws Exception {

        Workflow.Sequential wf = new Workflow.Sequential();
        TestItem job1 = new TestItem();
        TestItem job2 = new TestItem();

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
        assertEquals(ProcessState.COMPLETED, job1.getStatus().getState());
        assertEquals(ProcessState.SCHEDULED, job2.getStatus().getState());
        assertEquals(ProcessState.RUNNING, wf.getStatus().getState());

        incLifeStep(job1, job2);
        assertEquals(ProcessState.COMPLETED, job1.getStatus().getState());
        assertEquals(ProcessState.RUNNING, job2.getStatus().getState());
        assertEquals(ProcessState.RUNNING, wf.getStatus().getState());

        incLifeStep(job1, job2);
        assertEquals(ProcessState.COMPLETED, job1.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, job2.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, wf.getStatus().getState());

        incLifeStep(job1, job2);
        assertEquals(ProcessState.COMPLETED, job1.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, job2.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, wf.getStatus().getState());
    }

    @Test
    public void testParallel() throws Exception {

        Workflow.Parallel wf = new Workflow.Parallel();

        TestItem job1 = new TestItem();
        TestItem job2 = new TestItem();

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
        assertEquals(ProcessState.COMPLETED, job2.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, wf.getStatus().getState());

        incLifeStep(job1, job2);
        assertEquals(ProcessState.COMPLETED, job1.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, job2.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, wf.getStatus().getState());
    }

    @Test
    public void testMixed() throws Exception {

        Workflow.Sequential wf = new Workflow.Sequential();
        TestItem job1 = new TestItem();
        TestItem job2 = new TestItem();
        TestItem job3 = new TestItem();
        TestItem job4 = new TestItem();

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
        assertEquals(ProcessState.RUNNING, job3.getStatus().getState());
        assertEquals(ProcessState.UNKNOWN, job4.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, wfp.getStatus().getState());
        assertEquals(ProcessState.RUNNING, wfs.getStatus().getState());
        assertEquals(ProcessState.RUNNING, wf.getStatus().getState());

        incLifeStep(job1, job2, job3, job4);
        assertEquals(ProcessState.COMPLETED, job1.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, job2.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, job3.getStatus().getState());
        assertEquals(ProcessState.SCHEDULED, job4.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, wfp.getStatus().getState());
        assertEquals(ProcessState.RUNNING, wfs.getStatus().getState());
        assertEquals(ProcessState.RUNNING, wf.getStatus().getState());

        incLifeStep(job1, job2, job3, job4);
        assertEquals(ProcessState.COMPLETED, job1.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, job2.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, job3.getStatus().getState());
        assertEquals(ProcessState.RUNNING, job4.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, wfp.getStatus().getState());
        assertEquals(ProcessState.RUNNING, wfs.getStatus().getState());
        assertEquals(ProcessState.RUNNING, wf.getStatus().getState());

        incLifeStep(job1, job2, job3, job4);
        assertEquals(ProcessState.COMPLETED, job1.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, job2.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, job3.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, job4.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, wfp.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, wfs.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, wf.getStatus().getState());

        incLifeStep(job1, job2, job3, job4);
        assertEquals(ProcessState.COMPLETED, job1.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, job2.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, job3.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, job4.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, wfp.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, wfs.getStatus().getState());
        assertEquals(ProcessState.COMPLETED, wf.getStatus().getState());
    }

    static void incLifeStep(TestItem... jobs) {
        for (TestItem job : jobs) {
            job.incLifeStep();
        }
    }

    private static class TestItem extends Workflow {
        private boolean submitted;

        void incLifeStep() {
            if (submitted) {
                ProcessStatus status = getStatus();
                if (status.equals(ProcessStatus.UNKNOWN)) {
                    setStatus(new ProcessStatus(ProcessState.SCHEDULED));
                } else if (status.equals(ProcessStatus.SCHEDULED)) {
                    setStatus(new ProcessStatus(ProcessState.RUNNING, 0.5f));
                } else if (status.equals(new ProcessStatus(ProcessState.RUNNING, 0.5f))) {
                    setStatus(new ProcessStatus(ProcessState.COMPLETED));
                }
            }
        }

        @Override
        public void submit() {
            submitted = true;
        }

    }
}
