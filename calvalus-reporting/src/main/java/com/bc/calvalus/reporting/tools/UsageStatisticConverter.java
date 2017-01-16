package com.bc.calvalus.reporting.tools;

import com.bc.calvalus.reporting.exceptions.ExtractionException;
import com.bc.calvalus.reporting.ws.UsageStatistic;
import com.bc.calvalus.reporting.ws.UsageStatisticT2;

import java.util.List;

/**
 * @author hans
 */
public interface UsageStatisticConverter {

    /**
     * This method returns {@link UsageStatistic} of a single job, which is specified as jobId. It is
     * assumed that jobId is unique.
     *
     * @param jobId The requested job identifier
     *
     * @return UsageStatistic of the specified job
     */
    UsageStatisticT2 extractSingleStatistic(String jobId) throws ExtractionException;

    /**
     * This method returns all available {@link UsageStatistic}
     *
     * @return All {@link UsageStatistic}
     */
    List<UsageStatisticT2> extractAllStatistics() throws ExtractionException;

}
