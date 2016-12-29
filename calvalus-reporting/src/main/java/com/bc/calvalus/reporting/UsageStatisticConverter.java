package com.bc.calvalus.reporting;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
public class UsageStatisticConverter {

    private List<String[]> rawRecords;

    public UsageStatisticConverter(List<String[]> rawRecords) {
        this.rawRecords = rawRecords;
    }

    public UsageStatistic extractSingleStatistic(String jobId) {
        for (int i = 1; i < rawRecords.size(); i++) {
            String[] singleRecord = rawRecords.get(i);
            UsageStatistic usageStatistic = parseSingleUsageStatistic(singleRecord);
            if (jobId.equalsIgnoreCase(usageStatistic.getJobId())) {
                return usageStatistic;
            }
        }
        return new NullUsageStatistic();
    }

    public List<UsageStatistic> extractAllStatistics() {
        List<UsageStatistic> usageStatistics = new ArrayList<>();
        for (int i = 1; i < rawRecords.size(); i++) {
            String[] singleRecord = rawRecords.get(i);
            usageStatistics.add(parseSingleUsageStatistic(singleRecord));
        }
        return usageStatistics;
    }

    private UsageStatistic parseSingleUsageStatistic(String[] singleRecord) {
        return new UsageStatistic(singleRecord[0],
                                  singleRecord[1],
                                  Long.parseLong(singleRecord[2]),
                                  Long.parseLong(singleRecord[3]),
                                  singleRecord[4],
                                  Long.parseLong(singleRecord[5]),
                                  Long.parseLong(singleRecord[6]),
                                  Long.parseLong(singleRecord[7]),
                                  Long.parseLong(singleRecord[8]),
                                  Long.parseLong(singleRecord[9]),
                                  Long.parseLong(singleRecord[10]),
                                  Long.parseLong(singleRecord[11]),
                                  Long.parseLong(singleRecord[12]),
                                  Long.parseLong(singleRecord[13])
        );
    }
}
