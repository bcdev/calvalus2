package com.bc.calvalus.reporting.tools;

import com.bc.calvalus.reporting.exceptions.ExtractionException;
import com.bc.calvalus.reporting.io.JSONExtractor;
import com.bc.calvalus.reporting.ws.UsageStatistic;

import java.io.IOException;
import java.util.List;

/**
 * @author hans
 */
public class UsageStatisticJsonConverter implements UsageStatisticConverter {

    private final JSONExtractor jsonExtractor;

    UsageStatisticJsonConverter(JSONExtractor jsonExtractor) {
        this.jsonExtractor = jsonExtractor;
    }

    @Override
    public UsageStatistic extractSingleStatistic(String jobId) throws ExtractionException {
        try {
            return jsonExtractor.getSingleStatistic(jobId);
        } catch (IOException exception) {
            throw new ExtractionException(exception);
        }
    }

    @Override
    public List<UsageStatistic> extractAllStatistics() throws ExtractionException {
        try {
            return jsonExtractor.getAllStatistics();
        } catch (IOException exception) {
            throw new ExtractionException(exception);
        }
    }
}
