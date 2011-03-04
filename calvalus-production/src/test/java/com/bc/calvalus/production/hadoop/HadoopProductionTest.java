package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProductionState;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.Test;

import static org.junit.Assert.*;

public class HadoopProductionTest {

    @Test
    public void testStateTransitionWithStaging() {
        /*
        JobID jobID = new JobID("34627598547", 6);
        HadoopProduction processing = new HadoopProduction("id", "name", jobID, true, null);

        assertEquals(ProductionState.UNKNOWN, processing.getStatus().getState());

        processing.setProductionStatus(new JobStatus(org.apache.hadoop.mapred.JobID.downgrade(jobID), 0.8f, 0.1f, JobStatus.RUNNING));
        assertEquals(ProductionState.IN_PROGRESS, processing.getStatus().getState());
        assertEquals((0.8f + 0.1f + 0.0f) / 3, processing.getStatus().getProgress(), 1e-5f);

        processing.setProductionStatus(new JobStatus(org.apache.hadoop.mapred.JobID.downgrade(jobID), 1f, 1f, JobStatus.SUCCEEDED));
        assertEquals(ProductionState.UNKNOWN, processing.getStatus().getState());
        assertEquals((1.0f + 1.0f + 0.0f) / 3, processing.getStatus().getProgress(), 1e-5f);

        processing.setStagingStatus(new ProductionStatus(ProductionState.WAITING, 0.0f));
        processing.setStatus(null);
        assertEquals(ProductionState.WAITING, processing.getStatus().getState());
        assertEquals((1.0f + 1.0f + 0.0f) / 3, processing.getStatus().getProgress(), 1e-5f);

        processing.setStagingStatus(new ProductionStatus(ProductionState.IN_PROGRESS, 0.5f));
        processing.setStatus(null);
        assertEquals(ProductionState.IN_PROGRESS, processing.getStatus().getState());
        assertEquals((1.0f + 1.0f + 0.5f) / 3, processing.getStatus().getProgress(), 1e-5f);

        processing.setStagingStatus(new ProductionStatus(ProductionState.COMPLETED, 1.0f));
        processing.setStatus(null);
        assertEquals(ProductionState.COMPLETED, processing.getStatus().getState());
        assertEquals((1.0f + 1.0f + 1.0f) / 3, processing.getStatus().getProgress(), 1e-5f);
        */
    }

    @Test
    public void testStateTransitionWithoutStaging() {
        JobID jobID = new JobID("34627598547", 6);
        HadoopProduction processing = new HadoopProduction("id", "name", jobID, false, null);

        assertEquals(ProductionState.UNKNOWN, processing.getProcessingStatus().getState());

        processing.setProductionStatus(new JobStatus(org.apache.hadoop.mapred.JobID.downgrade(jobID), 0.8f, 0.1f, JobStatus.RUNNING));
        assertEquals(ProductionState.IN_PROGRESS, processing.getProcessingStatus().getState());
        assertEquals((0.8f + 0.1f) / 2, processing.getProcessingStatus().getProgress(), 1e-5f);

        processing.setProductionStatus(new JobStatus(org.apache.hadoop.mapred.JobID.downgrade(jobID), 1f, 1f, JobStatus.SUCCEEDED));
        assertEquals(ProductionState.COMPLETED, processing.getProcessingStatus().getState());
        assertEquals(1f, processing.getProcessingStatus().getProgress(), 1e-5f);
    }
}
