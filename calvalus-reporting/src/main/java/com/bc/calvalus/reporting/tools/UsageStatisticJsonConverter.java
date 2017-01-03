package com.bc.calvalus.reporting.tools;

import com.bc.calvalus.reporting.io.JSONReader;
import com.bc.calvalus.reporting.ws.UsageStatistic;

import java.util.List;

/**
 * @author hans
 */
public class UsageStatisticJsonConverter implements UsageStatisticConverter {

    private final JSONReader jsonReader;

    public UsageStatisticJsonConverter(JSONReader jsonReader) {
        this.jsonReader = jsonReader;
    }

    @Override
    public UsageStatistic extractSingleStatistic(String jobId) {
        return jsonReader.getSingleStatistic(jobId);
    }

    @Override
    public List<UsageStatistic> extractAllStatistics() {
        return jsonReader.getAllStatistics();
    }
}
