package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.JobIdFormat;
import com.bc.calvalus.processing.ProcessingService;
import org.junit.Ignore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
    public String getDataInputPath() {
        return "hdfs://cvmaster00:9000/calvalus/eodata";
    }

    @Override
    public String getDataOutputPath() {
        return "hdfs://cvmaster00:9000/calvalus/output";
    }

    @Override
    public String getSoftwarePath() {
        return "hdfs://cvmaster00:9000/calvalus/software/0.5";
    }

    @Override
    public String[] listFilePaths(String dirPath) throws IOException {
        return new String[]{
                dirPath + "/entry1",
                dirPath + "/entry2",
                dirPath + "/entry3",
        };
    }

    @Override
    public InputStream open(String path) throws IOException {
        return new ByteArrayInputStream("dummy".getBytes());
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
