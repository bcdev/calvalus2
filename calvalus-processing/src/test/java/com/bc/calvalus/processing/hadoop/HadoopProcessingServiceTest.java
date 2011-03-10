package com.bc.calvalus.processing.hadoop;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HadoopProcessingServiceTest {

    @Test
    public void testStatusConversion() {
        ProcessStatus processStatus;
        JobID jobID = new JobID("34627598547", 6);

        processStatus = HadoopProcessingService.convertStatus(new JobStatus(org.apache.hadoop.mapred.JobID.downgrade(jobID), 0.8f, 0.1f, JobStatus.RUNNING));
        assertEquals(ProcessState.RUNNING, processStatus.getState());
        assertEquals((0.8f + 0.1f) / 2, processStatus.getProgress(), 1e-5f);

        processStatus = HadoopProcessingService.convertStatus(new JobStatus(org.apache.hadoop.mapred.JobID.downgrade(jobID), 1f, 1f, JobStatus.SUCCEEDED));
        assertEquals(ProcessState.COMPLETED, processStatus.getState());
        assertEquals(1f, processStatus.getProgress(), 1e-5f);
    }
}
