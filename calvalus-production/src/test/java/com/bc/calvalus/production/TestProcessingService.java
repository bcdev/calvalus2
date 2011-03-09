package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.JobIdFormat;
import com.bc.calvalus.processing.ProcessingService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Test implementation of ProductionStore.
 *
 * @author Norman
 */
public class TestProcessingService implements ProcessingService<String> {
    private HashMap<String,ProcessStatus> jobStatusMap = new HashMap<String, ProcessStatus>();

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
    public String[] listFilePaths(String dirPath) throws IOException {
        return new String[]{
                dirPath + "/F1.N1",
                dirPath + "/F2.N1",
                dirPath + "/F3.N1",
                dirPath + "/F4.N1",
        };
    }

    public void setJobStatus(String jobId, ProcessStatus status) {
        this.jobStatusMap.put(jobId, status);
    }

    @Override
    public Map<String, ProcessStatus> getJobStatusMap() throws IOException {
        return new HashMap<String, ProcessStatus>(jobStatusMap);
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
}
