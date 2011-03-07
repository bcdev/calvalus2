package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProductionState;
import com.bc.calvalus.production.ProductionStatus;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HadoopProcessingServiceTest {

    @Test
    public void testStatusConversion() {
        ProductionStatus productionStatus;
        JobID jobID = new JobID("34627598547", 6);

        productionStatus = HadoopProcessingService.convertStatus(new JobStatus(org.apache.hadoop.mapred.JobID.downgrade(jobID), 0.8f, 0.1f, JobStatus.RUNNING));
        assertEquals(ProductionState.IN_PROGRESS, productionStatus.getState());
        assertEquals((0.8f + 0.1f) / 2, productionStatus.getProgress(), 1e-5f);

        productionStatus = HadoopProcessingService.convertStatus(new JobStatus(org.apache.hadoop.mapred.JobID.downgrade(jobID), 1f, 1f, JobStatus.SUCCEEDED));
        assertEquals(ProductionState.COMPLETED, productionStatus.getState());
        assertEquals(1f, productionStatus.getProgress(), 1e-5f);
    }
}
