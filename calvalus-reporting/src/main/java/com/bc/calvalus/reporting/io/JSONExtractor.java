package com.bc.calvalus.reporting.io;

import com.bc.calvalus.reporting.ws.NullUsageStatistic;
import com.bc.calvalus.reporting.ws.UsageStatistic;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
public class JSONExtractor {

    public UsageStatistic getSingleStatistic(String jobId) throws IOException {
        List<UsageStatistic> usageStatistics = getAllStatistics();
        for (UsageStatistic usageStatistic : usageStatistics) {
            if (jobId.equalsIgnoreCase(usageStatistic.getJobId())) {
                return usageStatistic;
            }
        }
        return new NullUsageStatistic();
    }

    public List<UsageStatistic> getSingleUserStatistic(String userName) throws IOException {
        List<UsageStatistic> usageStatistics = getAllStatistics();
        List<UsageStatistic> singleUserStatistics = new ArrayList<>();
        for (UsageStatistic usageStatistic : usageStatistics) {
            if (userName.equalsIgnoreCase(usageStatistic.getUser())) {
                singleUserStatistics.add(usageStatistic);
            }
        }
        return singleUserStatistics;
    }

    public List<UsageStatistic> getAllStatistics() throws IOException {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(PropertiesWrapper.get("reporting.file"));
        String reportingJsonString = extractJsonString(inputStream);
        Gson gson = new Gson();
        return gson.fromJson(reportingJsonString,
                             new TypeToken<List<UsageStatistic>>() {
                             }.getType());
    }

    public Map<String, List<UsageStatistic>> getAllUserStatistic() throws IOException {
        List<UsageStatistic> allStatistics = getAllStatistics();
        ConcurrentHashMap<String, List<UsageStatistic>> concurrentHashMap = new ConcurrentHashMap<>();
        allStatistics.stream().forEach(p -> {
            String user = p.getUser();
            concurrentHashMap.computeIfAbsent(user, s -> {
                try {
                    return getSingleUserStatistic(s);
                } catch (IOException e) {

                }
                return null;
            });
        });
        return concurrentHashMap;
    }

    @NotNull
    private String extractJsonString(InputStream inputStream) throws IOException {
        String reportingJsonString = IOUtils.toString(inputStream);
        reportingJsonString = StringUtils.stripEnd(reportingJsonString.trim(), ",");
        reportingJsonString = "[" + reportingJsonString + "]";
        return reportingJsonString;
    }
}
