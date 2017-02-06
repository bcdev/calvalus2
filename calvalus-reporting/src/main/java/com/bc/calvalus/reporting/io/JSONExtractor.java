package com.bc.calvalus.reporting.io;

import com.bc.calvalus.reporting.ws.NullUsageStatistic;
import com.bc.calvalus.reporting.ws.UsageStatistic;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;

/**
 * @author hans
 */
public class JSONExtractor {

    private static final String INIT_FIRST_DAY = "01";


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
        ConcurrentHashMap<String, List<UsageStatistic>> groupUserUsageStatistic = new ConcurrentHashMap<>();
        allStatistics.stream().forEach(p -> {
            String user = p.getUser();
            groupUserUsageStatistic.computeIfAbsent(user, userName -> {
                try {
                    return getSingleUserStatistic(userName);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                throw new NullPointerException("The user %userName have no statistic information.");
            });
        });
        return groupUserUsageStatistic;
    }


    public Map<String, List<UsageStatistic>> getAllUsersStartEndDateStatistic(String start, String end) throws IOException {
        Predicate<Long> predicate = filterDateIntervals(start, end);
        List<UsageStatistic> allStatistics = getAllStatistics();
        return getUsageStatisticsIfUserNull(predicate, allStatistics);
    }

    public List<UsageStatistic> getSingleUserStartEndDateStatistic(String user, String startDate, String endDate) throws IOException {
        Predicate<Long> rangePredicate = filterDateIntervals(startDate, endDate);
        List<UsageStatistic> allStatistics = getAllStatistics();
        List<UsageStatistic> singleUserYearStatistic = getSingleUserRangeStatistic(rangePredicate, user, allStatistics);
        return singleUserYearStatistic;
    }

    public List<UsageStatistic> getSingleUserYearStatistic(String user, String year) throws IOException {
        Predicate<FilterUserTimeInterval> yearPredicate = FilterUserTimeInterval::filterYear;
        List<UsageStatistic> singleUserYearStatistic = getSingleUserDateStatistic(yearPredicate, user, year, INIT_FIRST_DAY, INIT_FIRST_DAY);
        return singleUserYearStatistic;
    }

    public List<UsageStatistic> getSingleUserYearMonthStatistic(String user, String year, String month) throws IOException {
        Predicate<FilterUserTimeInterval> yearMonthPredicate = FilterUserTimeInterval::filterMonth;
        List<UsageStatistic> singleUserYearStatistic = getSingleUserDateStatistic(yearMonthPredicate, user, year, month, INIT_FIRST_DAY);
        return singleUserYearStatistic;
    }

    public List<UsageStatistic> getSingleUserYearMonthDayStatistic(String user, String year, String month, String day) throws IOException {
        Predicate<FilterUserTimeInterval> ymdPredicate = FilterUserTimeInterval::filterDay;
        List<UsageStatistic> singleUserYearStatistic = getSingleUserDateStatistic(ymdPredicate, user, year, month, day);
        return singleUserYearStatistic;
    }


    private List<UsageStatistic> getSingleUserDateStatistic(Predicate<FilterUserTimeInterval> intervalPredicate,
                                                            String user,
                                                            String yr,
                                                            String mnth,
                                                            String dy) throws IOException {

        final ConcurrentHashMap<String, List<UsageStatistic>> extractUserDate = new ConcurrentHashMap<>();
        final List<UsageStatistic> userStatisticInYear = new ArrayList<>();
        extractUserDate.put(user, userStatisticInYear);
        getAllStatistics().forEach(usage -> extractUserDate.computeIfPresent(usage.getUser(), getUserFromYear(intervalPredicate, yr, mnth, dy, userStatisticInYear, usage)));
        return userStatisticInYear;
    }

    @NotNull
    private BiFunction<String, List<UsageStatistic>, List<UsageStatistic>> getUserFromYear(Predicate<FilterUserTimeInterval> intervalPredicate,
                                                                                           String yr, String mnth, String dy,
                                                                                           List<UsageStatistic> userStatisticInYear,
                                                                                           UsageStatistic usage) {
        return (stringKey, usageStatistics) -> {
            FilterUserTimeInterval filterUserTimeInterval = new FilterUserTimeInterval(usage.getFinishTime(), yr, mnth, dy);

            if (intervalPredicate.test(filterUserTimeInterval)) {
                userStatisticInYear.add(usage);
            }
            return userStatisticInYear;
        };
    }


    @NotNull
    private Predicate<Long> filterDateIntervals(final String startDate, final String endDate) {
        return aLong -> {
            Instant end = LocalDate.parse(startDate).atTime(LocalTime.MAX).toInstant(ZoneOffset.UTC);
            Instant start = LocalDate.parse(endDate).atStartOfDay().toInstant(ZoneOffset.UTC);

            Instant instant = new Date(aLong).toInstant();
            if (instant.isAfter(start) && instant.isBefore(end)) {
                return true;
            }
            return false;
        };
    }


    private List<UsageStatistic> getSingleUserRangeStatistic(Predicate<Long> predTime, String userName, List<UsageStatistic> allStatisticUsage) {
        ConcurrentHashMap<String, List<UsageStatistic>> filterUserWithDate = new ConcurrentHashMap<>();
        List<UsageStatistic> userStatisticInYear = new ArrayList<>();
        filterUserWithDate.put(userName, userStatisticInYear);

        allStatisticUsage.forEach(usageStatistic -> getUsageStatisticsIfUserNotNull(predTime, filterUserWithDate, userStatisticInYear, usageStatistic));
        return userStatisticInYear;
    }


    private List<UsageStatistic> getUsageStatisticsIfUserNotNull(Predicate<Long> intervalPredicate,
                                                                 ConcurrentHashMap<String, List<UsageStatistic>> filterUserWithDate,
                                                                 List<UsageStatistic> userStatisticInYear,
                                                                 UsageStatistic usageStatistic) {
        return filterUserWithDate.computeIfPresent(usageStatistic.getUser(), (s, usageStatistics) -> {
            if (intervalPredicate.test(usageStatistic.getFinishTime())) {
                userStatisticInYear.add(usageStatistic);
            }
            return userStatisticInYear;
        });
    }

    private Map<String, List<UsageStatistic>> getUsageStatisticsIfUserNull(Predicate<Long> intervalPredicate, List<UsageStatistic> allStatistics) {
        ConcurrentHashMap<String, List<UsageStatistic>> groupUserUsageStatistic = new ConcurrentHashMap<>();
        List<UsageStatistic> usageStatisticList = allStatistics.stream().filter(p -> intervalPredicate.test(p.getFinishTime())).collect(Collectors.toList());

        usageStatisticList.forEach(p -> groupUserUsageStatistic.computeIfAbsent(p.getUser(), stringKey ->
                getSingleUserRangeStatistic(intervalPredicate, stringKey, usageStatisticList)
        ));
        return groupUserUsageStatistic;
    }

    @NotNull
    private String extractJsonString(InputStream inputStream) throws IOException {
        String reportingJsonString = IOUtils.toString(inputStream);
        reportingJsonString = StringUtils.stripEnd(reportingJsonString.trim(), ",");
        reportingJsonString = "[" + reportingJsonString + "]";
        return reportingJsonString;
    }

    static class FilterUserTimeInterval {

        private Long finishTime;
        private final String year;
        private final String month;
        private final String day;

        public FilterUserTimeInterval(Long finishTime, String year, String month, String day) {
            this.finishTime = finishTime;
            this.year = year;
            this.month = month;
            this.day = day;
        }


        public boolean filterMonth() {
            Instant start = Instant.parse(String.format("%s-%s-01T00:00:00.00Z", year, month));
            Instant end = Instant.parse(String.format("%s-0%s-01T00:00:00.00Z", year, Long.parseLong(month) + 1));
            Instant instant = new Date(finishTime).toInstant();
            if (instant.isAfter(start) && instant.isBefore(end)) {
                return true;
            }
            return false;

        }

        public boolean filterDay() {
            Instant start = Instant.parse(String.format("%s-%s-%sT00:00:00.00Z", year, month, day));
            Instant end = Instant.parse(String.format("%s-%s-%sT23:59:00.00Z", year, month, day));
            Instant instant = new Date(finishTime).toInstant();
            if (instant.isAfter(start) && instant.isBefore(end)) {
                return true;
            }
            return false;
        }

        boolean filterYear() {
            Instant start = Instant.parse(String.format("%s-01-01T00:00:00.00Z", year));
            Instant end = Instant.parse(String.format("%d-01-01T00:00:00.00Z", Long.parseLong(year) + 1));
            Instant instant = new Date(finishTime).toInstant();
            if (instant.isAfter(start) && instant.isBefore(end)) {
                return true;
            }
            return false;
        }
    }

}
