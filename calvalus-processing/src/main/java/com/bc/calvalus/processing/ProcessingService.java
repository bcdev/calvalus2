package com.bc.calvalus.processing;

import com.bc.calvalus.commons.ProcessStatus;

import java.io.IOException;
import java.util.Map;

/**
 * Service offered by some processing system. Includes basic data access and job management.
 *
 * @author Norman
 */
public interface ProcessingService<JOBID> {

    JobIdFormat<JOBID> getJobIdFormat();

    String getDataInputPath();

    String getDataOutputPath();

    String[] listFilePaths(String dirPath) throws IOException;

    Map<JOBID, ProcessStatus> getJobStatusMap() throws IOException;

    boolean killJob(JOBID jobId) throws IOException;
}
