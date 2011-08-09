package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.JobIdFormat;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.processing.ProcessorDescriptor;
import org.junit.Ignore;

import java.io.IOException;
import java.util.HashMap;

/**
 * Test implementation of ProductionStore.
 *
 * @author Norman
 */
@Ignore
public class TestProcessingService implements ProcessingService<String> {
    private HashMap<String,ProcessStatus> jobStatusMap = new HashMap<String, ProcessStatus>();
    boolean closed;

    @Override
    public JobIdFormat<String> getJobIdFormat() {
        return JobIdFormat.STRING;
    }

    @Override
    public ProcessorDescriptor[] getProcessors(String filter) throws IOException {
        return new ProcessorDescriptor[0];
    }

    @Override
    public void updateStatuses() throws IOException {
    }

    @Override
    public ProcessStatus getJobStatus(String jobId) {
        return jobStatusMap.get(jobId);
    }

    public void setJobStatus(String jobId, ProcessStatus status) {
        this.jobStatusMap.put(jobId, status);
    }

    @Override
    public boolean killJob(String jobId) throws IOException {
        ProcessStatus processStatus = jobStatusMap.get(jobId);
        if (processStatus == null) {
            return false;
        }
        if (processStatus.isDone()) {
            return false;
        }
        jobStatusMap.put(jobId, new ProcessStatus(ProcessState.CANCELLED, processStatus.getProgress()));
        return true;
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }
}
