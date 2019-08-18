package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.JobIdFormat;
import com.bc.calvalus.processing.MaskDescriptor;
import com.bc.calvalus.processing.ProcessingService;
import org.junit.Ignore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Timer;

/**
 * Test implementation of ProductionStore.
 *
 * @author Norman
 */
@Ignore
public class TestProcessingService implements ProcessingService<String> {

    private HashMap<String, ProcessStatus> jobStatusMap = new HashMap<String, ProcessStatus>();
    private boolean closed;

    @Override
    public JobIdFormat<String> getJobIdFormat() {
        return JobIdFormat.STRING;
    }

    @Override
    public BundleDescriptor[] getBundles(String username, BundleFilter filter) throws IOException {
        return new BundleDescriptor[0];
    }

    @Override
    public MaskDescriptor[] getMasks(String userName) throws Exception {
        return new MaskDescriptor[0];
    }

    @Override
    public void updateStatuses(String username) throws IOException {
    }

    @Override
    public ProcessStatus getJobStatus(String jobId) {
        ProcessStatus status = jobStatusMap.get(jobId);
        return status != null ? status : ProcessStatus.UNKNOWN;
    }

    public void setJobStatus(String jobId, ProcessStatus status) {
        this.jobStatusMap.put(jobId, status);
    }

    @Override
    public boolean killJob(String username, String jobId) throws IOException {
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

    public boolean isClosed() {
        return closed;
    }

    @Override
    public String[][] loadRegionDataInfo(String username, String url) throws IOException {
        return new String[0][];
    }

    @Override
    public void invalidateBundleCache() {}

    @Override
    public Timer getTimer() {
        return null;
    }
}
