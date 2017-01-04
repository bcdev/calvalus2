package com.bc.calvalus.reporting.tools;

import com.bc.calvalus.reporting.ws.NullUsageStatistic;
import com.bc.calvalus.reporting.ws.UsageStatistic;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
public class UsageStatisticCsvConverter implements UsageStatisticConverter {

    private List<String[]> rawRecords;

    public UsageStatisticCsvConverter(List<String[]> rawRecords) {
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
                                  singleRecord[2],
                                  Long.parseLong(singleRecord[3]),
                                  Long.parseLong(singleRecord[4]),
                                  singleRecord[5],
                                  singleRecord[6],
                                  Integer.parseInt(singleRecord[7]),
                                  Integer.parseInt(singleRecord[8]),
                                  Long.parseLong(singleRecord[9]),
                                  Long.parseLong(singleRecord[10]),
                                  Long.parseLong(singleRecord[11]),
                                  Long.parseLong(singleRecord[12]),
                                  Long.parseLong(singleRecord[13]),
                                  Long.parseLong(singleRecord[14]),
                                  Long.parseLong(singleRecord[15])
        );
    }
}
