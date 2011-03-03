package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProductionState;
import com.bc.calvalus.production.ProductionStatus;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.Test;

import static org.junit.Assert.*;

public class HadoopProductionTest {

    @Test
    public void testStateTransitionWithStaging() {
        JobID jobID = new JobID("34627598547", 6);
        HadoopProduction processing = new HadoopProduction("1", "processing", jobID, "/test", "NetCDF", true);

        assertEquals(ProductionState.UNKNOWN, processing.getStatus().getState());
        assertEquals(false, processing.isHadoopJobDone());

        processing.updateStatus(new JobStatus(org.apache.hadoop.mapred.JobID.downgrade(jobID), 0.8f, 0.1f, JobStatus.RUNNING));
        assertEquals(false, processing.isHadoopJobDone());
        assertEquals(ProductionState.IN_PROGRESS, processing.getStatus().getState());
        assertEquals((0.8f + 0.1f + 0.0f) / 3, processing.getStatus().getProgress(), 1e-5f);

        processing.updateStatus(new JobStatus(org.apache.hadoop.mapred.JobID.downgrade(jobID), 1f, 1f, JobStatus.SUCCEEDED));
        assertEquals(true, processing.isHadoopJobDone());
        assertEquals(ProductionState.UNKNOWN, processing.getStatus().getState());
        assertEquals((1.0f + 1.0f + 0.0f) / 3, processing.getStatus().getProgress(), 1e-5f);

        processing.setStagingStatus(new ProductionStatus(ProductionState.WAITING, 0.0f));
        processing.updateStatus(null);
        assertEquals(true, processing.isHadoopJobDone());
        assertEquals(ProductionState.WAITING, processing.getStatus().getState());
        assertEquals((1.0f + 1.0f + 0.0f) / 3, processing.getStatus().getProgress(), 1e-5f);

        processing.setStagingStatus(new ProductionStatus(ProductionState.IN_PROGRESS, 0.5f));
        processing.updateStatus(null);
        assertEquals(true, processing.isHadoopJobDone());
        assertEquals(ProductionState.IN_PROGRESS, processing.getStatus().getState());
        assertEquals((1.0f + 1.0f + 0.5f) / 3, processing.getStatus().getProgress(), 1e-5f);

        processing.setStagingStatus(new ProductionStatus(ProductionState.COMPLETED, 1.0f));
        processing.updateStatus(null);
        assertEquals(true, processing.isHadoopJobDone());
        assertEquals(ProductionState.COMPLETED, processing.getStatus().getState());
        assertEquals((1.0f + 1.0f + 1.0f) / 3, processing.getStatus().getProgress(), 1e-5f);
    }

    @Test
    public void testStateTransitionWithoutStaging() {
        JobID jobID = new JobID("34627598547", 6);
        HadoopProduction processing = new HadoopProduction("1", "processing", jobID, "/test", "NetCDF", false);

        assertEquals(ProductionState.UNKNOWN, processing.getStatus().getState());

        processing.updateStatus(new JobStatus(org.apache.hadoop.mapred.JobID.downgrade(jobID), 0.8f, 0.1f, JobStatus.RUNNING));
        assertEquals(ProductionState.IN_PROGRESS, processing.getStatus().getState());
        assertEquals((0.8f + 0.1f) / 2, processing.getStatus().getProgress(), 1e-5f);

        processing.updateStatus(new JobStatus(org.apache.hadoop.mapred.JobID.downgrade(jobID), 1f, 1f, JobStatus.SUCCEEDED));
        assertEquals(ProductionState.COMPLETED, processing.getStatus().getState());
        assertEquals(1f, processing.getStatus().getProgress(), 1e-5f);
    }


}
