package com.bc.calvalus.reporting.tools;

import com.bc.calvalus.reporting.exceptions.ExtractionException;
import com.bc.calvalus.reporting.io.JSONReader;
import com.bc.calvalus.reporting.ws.UsageStatistic;

import java.io.IOException;
import java.util.List;

/**
 * @author hans
 */
public class UsageStatisticJsonConverter implements UsageStatisticConverter {

    private final JSONReader jsonReader;

    UsageStatisticJsonConverter(JSONReader jsonReader) {
        this.jsonReader = jsonReader;
    }

    @Override
    public UsageStatistic extractSingleStatistic(String jobId) {
        return jsonReader.getSingleStatistic(jobId);
    }

    @Override
    public List<UsageStatistic> extractAllStatistics() throws ExtractionException {
        try {
            return jsonReader.getAllStatistics();
        } catch (IOException exception) {
            throw new ExtractionException(exception);
        }
    }
}
