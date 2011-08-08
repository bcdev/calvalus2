package com.bc.calvalus.processing;

import com.bc.calvalus.commons.ProcessStatus;

import java.io.IOException;
import java.io.InputStream;

/**
 * Service offered by some processing system. Includes basic data access and job management.
 *
 * @author Norman
 */
public interface ProcessingService<JOBID> {

    JobIdFormat<JOBID> getJobIdFormat();

    /**
     * @param inputPath A relative or absolute data input path.
     * @return A fully qualified URI comprising the filesystem and absolute data input path.
     */
    String getDataInputPath(String inputPath);

    /**
     * @param outputPath A relative or absolute data output path.
     * @return A fully qualified URI comprising the filesystem and absolute data output path.
     */
    String getDataOutputPath(String outputPath);

    String getSoftwarePath();

    String[] listFilePaths(String dirPath) throws IOException;

    // todo - not needed, code that uses it should go into ProcessingService impl.
    InputStream open(String path) throws IOException;

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
