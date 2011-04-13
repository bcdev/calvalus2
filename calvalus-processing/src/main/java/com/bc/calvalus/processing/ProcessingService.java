package com.bc.calvalus.processing;

import com.bc.calvalus.commons.ProcessStatus;

import java.io.IOException;

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

    ProcessStatus getJobStatus(JOBID jobid);

    boolean killJob(JOBID jobId) throws IOException;

    // todo - actually the service shall update itself on a regular basis
    /**
     * A request to retrieve and update the status of all processes.
     */
    void updateStatuses() throws IOException;

    /**
     * Indicates the service will no longer be used.
     * Invocation has no additional effect if already closed.
     */
    void close() throws IOException;
}
