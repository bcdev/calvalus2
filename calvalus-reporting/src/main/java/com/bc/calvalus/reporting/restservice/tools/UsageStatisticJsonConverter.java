package com.bc.calvalus.reporting.restservice.tools;

import com.bc.calvalus.reporting.restservice.exceptions.ExtractionException;
import com.bc.calvalus.reporting.restservice.io.JSONExtractor;
import com.bc.calvalus.reporting.restservice.ws.UsageStatistic;
import com.bc.calvalus.reporting.restservice.ws.UsageStatisticT2;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
public class UsageStatisticJsonConverter implements UsageStatisticConverter {

    private final JSONExtractor jsonExtractor;

    public UsageStatisticJsonConverter(JSONExtractor jsonExtractor) {
        this.jsonExtractor = jsonExtractor;
    }

    @Override
    public UsageStatisticT2 extractSingleStatistic(String jobId, String date) throws ExtractionException {
        try {
            UsageStatistic singleStatistic = jsonExtractor.getSingleStatistic(jobId, date);
            return new UsageStatisticT2(singleStatistic);
        } catch (IOException exception) {
            throw new ExtractionException(exception);
        }
    }

    @Override
    public List<UsageStatisticT2> extractAllStatistics(String fileNameByDate) throws ExtractionException {
        try {
            List<UsageStatistic> usageStatistics = jsonExtractor.loadStatisticOf(fileNameByDate);
            List<UsageStatisticT2> usageStatisticT2List = new ArrayList<>();
            for (UsageStatistic usageStatistic : usageStatistics) {
                usageStatisticT2List.add(new UsageStatisticT2(usageStatistic));
            }
            return usageStatisticT2List;
        } catch (IOException exception) {
            throw new ExtractionException(exception);
        }
    }
}
