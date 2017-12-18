package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

public class ProductionTest {

    @Test
    public void testConstructor() throws Exception {
        Production production;
        JobID jobID = new JobID("34627598547", 6);

        production = new Production("9A3F", "Toasting", "outputHere", "stagingThere",
                                    false, new ProductionRequest("test", "ewa"),
                                    new MyWorkflowItem(jobID));
        assertEquals("9A3F", production.getId());
        assertEquals("Toasting", production.getName());

        assertEquals("outputHere", production.getOutputPath());
        assertNotNull(production.getIntermediateDataPath());
        assertEquals(0, production.getIntermediateDataPath().length);
        assertEquals("stagingThere", production.getStagingPath());
        assertEquals(false, production.isAutoStaging());
        assertEquals(1, production.getJobIds().length);
        assertEquals(jobID, production.getJobIds()[0]);
        assertEquals("test", production.getProductionRequest().getProductionType());
        assertEquals("ewa", production.getProductionRequest().getUserName());
        assertEquals(ProcessState.UNKNOWN, production.getProcessingStatus().getState());
        assertEquals(ProcessState.UNKNOWN, production.getStagingStatus().getState());

        String[] interDirs = new String[]{"a", "b"};
        production = new Production("9A3F", "Toasting", "outputHere", interDirs, "stagingThere",
                                    false, new ProductionRequest("test", "ewa"),
                                    new MyWorkflowItem(jobID));
        assertArrayEquals(interDirs, production.getIntermediateDataPath());
    }

    @Test
    public void testConstructorWithWorkflows() throws Exception {
        WorkflowItem wf1 = new MyHadoopWorkflowItem("u1", "dir1");
        WorkflowItem wf2 = new MyHadoopWorkflowItem("u1", "dir2");
        WorkflowItem wf3 = new MyHadoopWorkflowItem("u1", "dir3");
        Workflow.Sequential sequential = new Workflow.Sequential(wf1, wf2, wf3);

        Production production = new Production("9A3F", "Toasting", "outputHere", "stagingThere",
                                    false, new ProductionRequest("test", "ewa"),
                                    sequential);

        assertEquals("outputHere", production.getOutputPath());
        String[] intermediateDataPath = production.getIntermediateDataPath();
        assertNotNull(intermediateDataPath);
        assertEquals(3, intermediateDataPath.length);
        Arrays.sort(intermediateDataPath);
        assertEquals("dir1", intermediateDataPath[0]);
        assertEquals("dir2", intermediateDataPath[1]);
        assertEquals("dir3", intermediateDataPath[2]);

        production = new Production("9A3F", "Toasting", "dir3", "stagingThere",
                                    false, new ProductionRequest("test", "ewa"),
                                    sequential);

        assertEquals("dir3", production.getOutputPath());
        intermediateDataPath = production.getIntermediateDataPath();
        assertNotNull(intermediateDataPath);
        assertEquals(2, intermediateDataPath.length);
        Arrays.sort(intermediateDataPath);
        assertEquals("dir1", intermediateDataPath[0]);
        assertEquals("dir2", intermediateDataPath[1]);

        production = new Production("9A3F", "Toasting", null, "stagingThere",
                                    false, new ProductionRequest("test", "ewa"),
                                    sequential);

        assertEquals(null, production.getOutputPath());
        intermediateDataPath = production.getIntermediateDataPath();
        assertNotNull(intermediateDataPath);
        assertEquals(0, intermediateDataPath.length);
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

    private static class MyHadoopWorkflowItem extends HadoopWorkflowItem {


        final String outputDir;

        public MyHadoopWorkflowItem(String userName, String outputDir) {
            super(null, userName, null, null);
            this.outputDir = outputDir;
        }

        @Override
        public String getOutputDir() {
            return outputDir;
        }

        @Override
        protected void configureJob(Job job) throws IOException {
        }

        @Override
        protected String[][] getJobConfigDefaults() {
            return new String[0][];
        }
    }
}