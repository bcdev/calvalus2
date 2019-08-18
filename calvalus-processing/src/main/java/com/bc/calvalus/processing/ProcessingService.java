package com.bc.calvalus.processing;

import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.processing.hadoop.HadoopJobHook;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Timer;

/**
 * Service offered by some processing system. Includes processor information and job management.
 *
 * @author Norman
 */
public interface ProcessingService<JobId> {

    /**
     * Gets the list of processor bundles available by the service.
     *
     * @param filter An filter to define which bundles shall be looked up.
     *
     * @return The list of processors supported by the service.
     *
     * @throws IOException if an I/O error occurs.
     */
    BundleDescriptor[] getBundles(String username, BundleFilter filter) throws Exception;

    /**
     * Gets the masks available by the service.
     *
     * @param userName The name of the user for which the masks shall be retrieved.
     *
     * @return The masks available by the service.
     *
     * @throws Exception if an error occurs.
     */
    MaskDescriptor[] getMasks(String userName) throws Exception;

    /**
     * @return The format used represent job IDs as plain text.
     */
    JobIdFormat<JobId> getJobIdFormat();

    /**
     * Gets status information for a processing job.
     *
     * @param jobId The job identifier.
     *
     * @return Job status information.
     */
    ProcessStatus getJobStatus(JobId jobId);

    /**
     * Kill a processing job.
     *
     * @param jobId The job identifier.
     *
     * @return {@code true} if the job could be killed.
     *
     * @throws java.io.IOException if an I/O error occurs
     */
    boolean killJob(String username, JobId jobId) throws Exception;

    /**
     * A request to retrieve and update the status of all processes.
     * (todo - actually the service shall update itself on a regular basis (nf))
     *
     * @throws java.io.IOException if an I/O error occurs
     */
    void updateStatuses(String username) throws Exception;

    /**
     * Indicates the service will no longer be used.
     * Invocation has no additional effect if already closed.
     *
     * @throws java.io.IOException if an I/O error occurs
     */
    void close() throws Exception;

    /**
     * Load details about the region data.
     */
    public String[][] loadRegionDataInfo(String username, String url) throws IOException;

    void invalidateBundleCache();

    Timer getTimer();
}
