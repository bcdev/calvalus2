package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProductionStatus;

import java.io.IOException;
import java.util.Map;

/**
 * Service offered by some processing system. Includes basic data access and job management.
 *
 * @author Norman
 */
public interface ProcessingService {

    String getDataArchiveRootPath();

    String getDataOutputRootPath();

    String[] listFilePaths(String dirPath) throws IOException;

    Map<Object, ProductionStatus> getJobStatusMap() throws IOException;

    boolean killJob(Object jobId) throws IOException;
}
