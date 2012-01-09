package com.bc.calvalus.commons;


import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.*;

public class WorkflowTest {

    @Test
    public void testSequential() throws Exception {

        Workflow.Sequential wf = new Workflow.Sequential();
        LifeStepWorkflowItem job1 = new LifeStepWorkflowItem();
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

        assertEquals(1, job1.getSubmitCount());
        assertEquals(1, job2.getSubmitCount());
    }

    @Test
    public void testSequential_empty() throws Exception {

        Workflow.Sequential wf = new Workflow.Sequential();
        assertEquals(ProcessState.UNKNOWN, wf.getStatus().getState());
        wf.submit();
        assertEquals(ProcessState.COMPLETED, wf.getStatus().getState());
    }

    @Test
    public void testParallel_empty() throws Exception {

        Workflow.Parallel wf = new Workflow.Parallel();
        assertEquals(ProcessState.UNKNOWN, wf.getStatus().getState());
        wf.submit();
        assertEquals(ProcessState.COMPLETED, wf.getStatus().getState());
    }

    @Test
    public void testParallel() throws Exception {

        Workflow.Parallel wf = new Workflow.Parallel();

        LifeStepWorkflowItem job1 = new LifeStepWorkflowItem();
        LifeStepWorkflowItem job2 = new LifeStepWorkflowItem();

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

        assertEquals(1, job1.getSubmitCount());
        assertEquals(1, job2.getSubmitCount());
    }

    @Test
    public void testMixed() throws Exception {

        Workflow.Sequential wf = new Workflow.Sequential();
        LifeStepWorkflowItem job1 = new LifeStepWorkflowItem();
        LifeStepWorkflowItem job2 = new LifeStepWorkflowItem();
        LifeStepWorkflowItem job3 = new LifeStepWorkflowItem();
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

        assertEquals(1, job1.getSubmitCount());
        assertEquals(1, job2.getSubmitCount());
        assertEquals(1, job3.getSubmitCount());
        assertEquals(1, job4.getSubmitCount());
    }

    @Test
    public void testParallelStatusChangeAggregation() {
        LifeStepWorkflowItem job1 = new LifeStepWorkflowItem();
        LifeStepWorkflowItem job2 = new LifeStepWorkflowItem();

        Workflow wf = new Workflow.Parallel();
        wf.add(job1, job2);

        job1.setStatus(new ProcessStatus(ProcessState.RUNNING, 0.4F));
        job2.setStatus(new ProcessStatus(ProcessState.RUNNING, 0.2F));
        assertEquals(new ProcessStatus(ProcessState.RUNNING, 0.3F), wf.getStatus());

        job2.setStatus(new ProcessStatus(ProcessState.RUNNING, 0.8F));
        assertEquals(new ProcessStatus(ProcessState.RUNNING, 0.6F), wf.getStatus());

        job1.setStatus(new ProcessStatus(ProcessState.RUNNING, 0.8F));
        assertEquals(new ProcessStatus(ProcessState.RUNNING, 0.8F), wf.getStatus());

    }

    @Test
    public void testSequentialStatusChangeAggregation() {
        LifeStepWorkflowItem job1 = new LifeStepWorkflowItem();
        LifeStepWorkflowItem job2 = new LifeStepWorkflowItem();

        Workflow wf = new Workflow.Sequential();
        wf.add(job1, job2);

        job2.setStatus(new ProcessStatus(ProcessState.RUNNING, 0.3F));
        job1.setStatus(new ProcessStatus(ProcessState.UNKNOWN));

        assertEquals(new ProcessStatus(ProcessState.RUNNING, 0.15F), wf.getStatus());
    }

    @Test
    public void testTimeStampChange() {
        LifeStepWorkflowItem job = new LifeStepWorkflowItem();
        job.setTime(new Date());

        assertNull(job.getSubmitTime());
        assertNull(job.getStartTime());
        assertNull(job.getStopTime());

        job.setStatus(new ProcessStatus(ProcessState.SCHEDULED));
        assertNotNull(job.getSubmitTime());
        assertNull(job.getStartTime());
        assertNull(job.getStopTime());

        job.setStatus(new ProcessStatus(ProcessState.RUNNING));
        assertNotNull(job.getSubmitTime());
        assertNotNull(job.getStartTime());
        assertNull(job.getStopTime());

        job.setStatus(new ProcessStatus(ProcessState.RUNNING, 0.7f));
        assertNotNull(job.getSubmitTime());
        assertNotNull(job.getStartTime());
        assertNull(job.getStopTime());

        job.setStatus(new ProcessStatus(ProcessState.COMPLETED));
        assertNotNull(job.getSubmitTime());
        assertNotNull(job.getStartTime());
        assertNotNull(job.getStopTime());

        job.setStatus(new ProcessStatus(ProcessState.UNKNOWN));
        assertNotNull(job.getSubmitTime());
        assertNotNull(job.getStartTime());
        assertNotNull(job.getStopTime());
    }

    @Test
    public void testTimeTrackingForSingleJob() {
        LifeStepWorkflowItem job = new LifeStepWorkflowItem();
        Date submitTime = new Date(2000);
        Date startTime = new Date(3000);
        Date stopTime = new Date(5000);

        job.submit();
        assertWfTime(job, null, null, null);

        job.setTime(submitTime);
        incLifeStep(job);
        assertWfTime(job, submitTime, null, null);

        job.setTime(startTime);
        incLifeStep(job);
        assertWfTime(job, submitTime, startTime, null);

        job.setTime(stopTime);
        incLifeStep(job);
        assertWfTime(job, submitTime, startTime, stopTime);
    }


    @Test
    public void testTimeTrackingForSequentialWorkflow() throws WorkflowException {
        Workflow.Sequential wf = new Workflow.Sequential();
        LifeStepWorkflowItem job1 = new LifeStepWorkflowItem();
        LifeStepWorkflowItem job2 = new LifeStepWorkflowItem();

        wf.add(job1, job2);
        wf.submit();

        Date submitTime1 = new Date(2000);
        Date startTime1 = new Date(3000);
        Date stopTime1 = new Date(5000);
        Date submitTime2 = new Date(5000);
        Date startTime2 = new Date(7000);
        Date stopTime2 = new Date(9000);


        assertWfTime(job1, null, null, null);
        assertWfTime(job2, null, null, null);
        assertWfTime(wf, null, null, null);

        setTime(submitTime1, job1, job2);
        incLifeStep(job1, job2);
        assertWfTime(job1, submitTime1, null, null);
        assertWfTime(job2, null, null, null);
        assertWfTime(wf, submitTime1, null, null);

        setTime(startTime1, job1, job2);
        incLifeStep(job1, job2);
        assertWfTime(job1, submitTime1, startTime1, null);
        assertWfTime(job2, null, null, null);
        assertWfTime(wf, submitTime1, startTime1, null);

        setTime(stopTime1, job1, job2);
        incLifeStep(job1, job2);
        assertWfTime(job1, submitTime1, startTime1, stopTime1);
        assertWfTime(job2, submitTime2, null, null);
        assertWfTime(wf, submitTime1, startTime1, null);

        setTime(startTime2, job1, job2);
        incLifeStep(job1, job2);
        assertWfTime(job1, submitTime1, startTime1, stopTime1);
        assertWfTime(job2, submitTime2, startTime2, null);
        assertWfTime(wf, submitTime1, startTime1, null);

        setTime(stopTime2, job1, job2);
        incLifeStep(job1, job2);
        assertWfTime(job1, submitTime1, startTime1, stopTime1);
        assertWfTime(job2, submitTime2, startTime2, stopTime2);
        assertWfTime(wf, submitTime1, startTime1, stopTime2);

        incLifeStep(job1, job2);
        assertWfTime(job1, submitTime1, startTime1, stopTime1);
        assertWfTime(job2, submitTime2, startTime2, stopTime2);
        assertWfTime(wf, submitTime1, startTime1, stopTime2);
    }

    private static void assertWfTime(WorkflowItem workflow, Date submitTime, Date startTime, Date stopTime) {
        assertEquals("submitTime", submitTime, workflow.getSubmitTime());
        assertEquals("startTime", startTime, workflow.getStartTime());
        assertEquals("stopTime", stopTime, workflow.getStopTime());
    }

    static void incLifeStep(LifeStepWorkflowItem... jobs) {
        for (LifeStepWorkflowItem job : jobs) {
            job.incLifeStep();
        }
    }

    static void setTime(Date time, LifeStepWorkflowItem... jobs) {
        for (LifeStepWorkflowItem job : jobs) {
            job.setTime(time);
        }
    }

}
