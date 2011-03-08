package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.JobIdFormat;
import com.bc.calvalus.processing.ProcessingService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
* todo - add api doc
*
* @author Norman Fomferra
*/
public class TestProcessingService implements ProcessingService {
    @Override
    public JobIdFormat getJobIdFormat() {
        return JobIdFormat.TEXT;
    }

    @Override
    public String getDataArchiveRootPath() {
        return "hdfs://cvmaster00:9000/calvalus/eodata";
    }

    @Override
    public String getDataOutputRootPath() {
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

    @Override
    public Map<Object, ProcessStatus> getJobStatusMap() throws IOException {
        return new HashMap<Object, ProcessStatus>();
    }

    @Override
    public boolean killJob(Object jobId) throws IOException {
        return false;
    }
}
