package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessState;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.Test;

import static org.junit.Assert.*;

public class ProductionTest {

    @Test
    public void testConstructor() throws Exception {
        Production production;
        JobID jobID = new JobID("34627598547", 6);

        production = new Production("9A3F", "Toasting", null, null,
                                    false, new ProductionRequest("test", "ewa"),
                                    new MyWorkflowItem(jobID));
        assertEquals("9A3F", production.getId());
        assertEquals("Toasting", production.getName());
        assertEquals(null, production.getStagingPath());
        assertEquals(false, production.isAutoStaging());
        assertEquals(1, production.getJobIds().length);
        assertEquals(jobID, production.getJobIds()[0]);
        assertEquals("test", production.getProductionRequest().getProductionType());
        assertEquals("ewa", production.getProductionRequest().getUserName());
        assertEquals(ProcessState.UNKNOWN, production.getProcessingStatus().getState());
        assertEquals(ProcessState.UNKNOWN, production.getStagingStatus().getState());
    }

    @Test
    public void testGetJobIds() throws Exception {
        Production production = new Production("9A3F", "Toasting", null, null,
                                               false, new ProductionRequest("test", "ewa"),
                                               new MyWorkflowItem(new JobID("34627985F47", 4)));

        assertArrayEquals(new Object[]{new JobID("34627985F47", 4)}, production.getJobIds());
    }

    @Test
    public void testCreateId() {
        String id = Production.createId("level4");
        assertTrue(id.contains("level4"));
        assertFalse(id.equals(Production.createId("level4")));
    }

    private static class MyWorkflowItem extends TestWorkflowItem<JobID> {
        MyWorkflowItem(JobID id) {
            super(id);
        }
    }
}