package com.bc.calvalus.processing;

import com.bc.calvalus.commons.ProcessStatus;

import java.io.IOException;

/**
 * Service offered by some processing system. Includes processor information and job management.
 *
 * @author Norman
 */
public interface ProcessingService<JobId> {

    /**
     * Gets the list of processors supported by the service.
     *
     *
     * @param filter An optional filter expression (unused)
     * @return The list of processors supported by the service.
     * @throws IOException if an I/O error occurs.
     */
    BundleDescriptor[] getBundles(String filter) throws Exception;

    /**
     * @return The format used represent job IDs as plain text.
     */
    JobIdFormat<JobId> getJobIdFormat();

    /**
     * Gets status information for a processing job.
     * @param jobId The job identifier.
     * @return Job status information.
     */
    ProcessStatus getJobStatus(JobId jobId);

    /**
     * Kill a processing job.
     * @param jobId The job identifier.
     * @return {@code true} if the job could be killed.
     *
     * @throws java.io.IOException if an I/O error occurs
     */
    boolean killJob(JobId jobId) throws Exception;

    /**
     * A request to retrieve and update the status of all processes.
     * (todo - actually the service shall update itself on a regular basis (nf))
     *
     * @throws java.io.IOException if an I/O error occurs
     */
    void updateStatuses() throws Exception;

    /**
     * Indicates the service will no longer be used.
     * Invocation has no additional effect if already closed.
     *
     * @throws java.io.IOException if an I/O error occurs
     */
    void close() throws Exception;
}
