package com.bc.calvalus.reporting.restservice.io;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.common.NullUsageStatistic;
import com.bc.calvalus.reporting.common.UsageStatistic;
import com.bc.calvalus.reporting.restservice.exceptions.DatabaseFileNotFoundException;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author hans , muhammad
 */
public class JSONExtractor {

    private static final String INIT_FIRST_DAY = "01";
    private static final int DAY_OF_MONTH = 1;
    private static final Logger LOGGER = CalvalusLogger.getLogger();
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

    private String databaseFolderPath = PropertiesWrapper.get("reporting.folder.path");

    public List<UsageStatistic> loadStatisticOf(String date) throws IOException, DatabaseFileNotFoundException {
        String[] fileList = Paths.get(databaseFolderPath).toFile().list();
        if (fileList != null) {
            Predicate<String> filterFileLogBtwDate = filterFileLogBtwDate(date);
            Optional<String> fileNameOptional = Arrays.stream(fileList).filter(filterFileLogBtwDate).findFirst();
            String fileName = fileNameOptional.orElse("");
            String fullPathToLogFile = Paths.get(databaseFolderPath).resolve(fileName).toString();
            return getAllStatistics(fullPathToLogFile);
        } else {
            throw new DatabaseFileNotFoundException("Database path '" + databaseFolderPath + "' is empty.");
        }
    }

    public Map<String, List<UsageStatistic>> getAllUserUsageStatistic(String date)
                throws IOException, DatabaseFileNotFoundException {
        List<UsageStatistic> allStatistics = loadStatisticOf(date);
        ConcurrentHashMap<String, List<UsageStatistic>> groupUserUsageStatistic = new ConcurrentHashMap<>();
        allStatistics.forEach(p -> {
            String user = p.getUser();
            groupUserUsageStatistic.computeIfAbsent(user, userName -> {
                List<UsageStatistic> singleUserStatistic;
                try {
                    singleUserStatistic = getSingleUserStatistic(userName, date);
                } catch (IOException exception) {
                    throw new UncheckedIOException(exception);
                }
                return singleUserStatistic;
            });
        });
        return groupUserUsageStatistic;
    }

    public List<UsageStatistic> getUsageStatisticsSince(String startDate)
                throws IOException, DatabaseFileNotFoundException {
        List<UsageStatistic> allStatistics = loadStatisticFromDate(startDate);
        List<UsageStatistic> statisticsSinceStartDate = new ArrayList<>();
        for (UsageStatistic usageStatistic : allStatistics) {
            long finishTime = usageStatistic.getFinishTime();
            TemporalAccessor accessor = TIME_FORMATTER.parse(startDate);
            Date date = Date.from(Instant.from(accessor));
            long dateMillis = date.getTime();
            if (finishTime >= dateMillis) {
                statisticsSinceStartDate.add(usageStatistic);
            }
        }
        return statisticsSinceStartDate;
    }

    public Map<String, List<UsageStatistic>> getAllUserUsageBetween(String startDate, String endDate) throws
                                                                                                      IOException {
        Predicate<Long> predicate = filterDateIntervals(startDate, endDate);
        List<UsageStatistic> allStatistics = loadStatisticBetweenDate(startDate, endDate);
        List<UsageStatistic> usageStatisticList = allStatistics.stream().filter(
                    p -> predicate.test(p.getFinishTime())).collect(Collectors.toList());

        ConcurrentHashMap<String, List<UsageStatistic>> groupUserUsageStatistic = new ConcurrentHashMap<>();
        usageStatisticList.forEach(p -> groupUserUsageStatistic.computeIfAbsent(p.getUser(), stringKey ->
                    filterUser(stringKey, usageStatisticList)
        ));
        return groupUserUsageStatistic;
    }

    public Map<String, List<UsageStatistic>> getAllDateUsageBetween(String startDate, String endDate)
                throws IOException {
        List<UsageStatistic> allStatistics = loadStatisticBetweenDate(startDate, endDate);
        Set<String> dates = getDatesBetween(startDate, endDate);
        Map<String, List<UsageStatistic>> usageWithDate = new HashMap<>();

        dates.forEach(date -> {
            Predicate<Long> predicate = filterDateIntervals(date, date);
            List<UsageStatistic> usageStatisticList = allStatistics
                        .stream()
                        .filter(p -> predicate.test(p.getFinishTime()))
                        .collect(Collectors.toList());
            usageWithDate.put(date, usageStatisticList);
        });

        return usageWithDate;
    }

    public Map<String, List<UsageStatistic>> getAllQueueUsageBetween(String startDate, String endDate)
                throws IOException {
        Predicate<Long> predicate = filterDateIntervals(startDate, endDate);
        List<UsageStatistic> allStatistics = loadStatisticBetweenDate(startDate, endDate);
        List<UsageStatistic> usageStatisticList = allStatistics.stream().filter(
                    p -> predicate.test(p.getFinishTime())).collect(Collectors.toList());
        ConcurrentHashMap<String, List<UsageStatistic>> groupUserUsageStatistic = new ConcurrentHashMap<>();

        usageStatisticList.forEach(p -> groupUserUsageStatistic.computeIfAbsent(p.getQueue(), queue ->
                    filterQueue(queue, usageStatisticList)
        ));
        return groupUserUsageStatistic;
    }

    public UsageStatistic getSingleStatistic(String jobId, String date)
                throws IOException, DatabaseFileNotFoundException {
        List<UsageStatistic> usageStatistics = loadStatisticOf(date);
        for (UsageStatistic usageStatistic : usageStatistics) {
            if (jobId.equalsIgnoreCase(usageStatistic.getJobId())) {
                return usageStatistic;
            }
        }
        return new NullUsageStatistic();
    }

    public List<UsageStatistic> getSingleUserStatistic(String userName, String date)
                throws IOException, DatabaseFileNotFoundException {
        List<UsageStatistic> usageStatistics = loadStatisticOf(date);
        List<UsageStatistic> singleUserStatistics = new ArrayList<>();
        for (UsageStatistic usageStatistic : usageStatistics) {
            if (userName.equalsIgnoreCase(usageStatistic.getUser())) {
                singleUserStatistics.add(usageStatistic);
            }
        }
        return singleUserStatistics;
    }

    public List<UsageStatistic> getSingleUserUsageBetween(String user, String startDate, String endDate)
                throws IOException {
        Predicate<Long> rangePredicate = filterDateIntervals(startDate, endDate);
        List<UsageStatistic> allStatistics = loadStatisticBetweenDate(startDate, endDate);
        return getSingleUserRangeStatistic(rangePredicate, user, allStatistics);
    }

    public List<UsageStatistic> getSingleUserUsageInYear(String user, String year) throws IOException {
        String beginYrDate = String.format("%s-01-01", year);
        String endYrDate = String.format("%s-12-31", year);
        return getSingleUserUsageBetween(user, beginYrDate, endYrDate);
    }


    public List<UsageStatistic> getSingleUserUsageInYearMonth(String user, String year, String month)
                throws IOException, DatabaseFileNotFoundException {
        Predicate<FilterUserTimeInterval> yearMonthPredicate = FilterUserTimeInterval::filterMonth;
        return getSingleUserDate(yearMonthPredicate, user, year, month, INIT_FIRST_DAY);

    }

    public List<UsageStatistic> getSingleUserUsageYearMonthDay(String user, String year, String month,
                                                               String day)
                throws IOException, DatabaseFileNotFoundException {
        Predicate<FilterUserTimeInterval> ymdPredicate = FilterUserTimeInterval::filterDay;
        return getSingleUserDate(ymdPredicate, user, year, month, day);
    }

    Set<String> getDatesBetween(String start, String end) {
        LocalDateTime startOfDay = LocalDate.parse(start).atStartOfDay();
        LocalDateTime endOfDay = LocalDate.parse(end).atStartOfDay();
        long l = Duration.between(startOfDay, endOfDay).toDays();
        Set<String> dates = new TreeSet<>();
        for (int i = 0; i <= l; i++) {
            LocalDate localDate = startOfDay.plusDays(i).toLocalDate();
            dates.add(localDate.toString());
        }
        return dates;
    }

    Predicate<String> filterFileLogBtwDate(String startDate, String endDate) {
        return (String aLong) -> {

            Matcher matcher = groupMatchers(aLong);
            if (!matcher.find()) {
                return false;
            }
            String startDateFrmFileName = matcher.group(1);
            String endDateFrmFileName = matcher.group(3);


            LocalDate end = LocalDate.parse(endDateFrmFileName);
            LocalDate start = LocalDate.parse(startDateFrmFileName);

            LocalDate instantStartDT;
            LocalDate instantStartET;

            instantStartDT = getFirstDayOfMonth(startDate);
            instantStartET = getLastDayOfMonth(endDate);

            boolean startRange = (instantStartDT.isBefore(start) || instantStartDT.equals(start))
                                 && (instantStartET.isAfter(start) || instantStartET.equals(start));

            boolean endRange = (instantStartDT.isBefore(end) || instantStartDT.equals(end))
                               && (instantStartET.isAfter(end) || instantStartDT.equals(end));

            return startRange || endRange;
        };
    }

    Predicate<String> filterFileLogBtwDate(final String searchDate) {
        return aLong -> {
            Matcher matcher = groupMatchers(aLong);
            if (!matcher.find()) {
                return false;
            }
            String startDateFrmFileName = matcher.group(1);
            String endDateFrmFileName = matcher.group(3);


            LocalDate end = LocalDate.parse(endDateFrmFileName);
            LocalDate start = LocalDate.parse(startDateFrmFileName);

            LocalDate instant = LocalDate.parse(searchDate);
            return (start.isBefore(instant) || start.equals(instant)) && (end.isAfter(instant) || end.equals(instant));
        };
    }

    private List<UsageStatistic> loadStatisticBetweenDate(String startDate, String endDate) throws IOException {
        List<UsageStatistic> usageStatisticList = new ArrayList<>();
        String[] fileList = Paths.get(databaseFolderPath).toFile().list();
        if (fileList != null) {
            Predicate<String> filterFileLogBtwDate = filterFileLogBtwDate(startDate, endDate);
            List<String> fileListCollected = Arrays.stream(fileList).filter(filterFileLogBtwDate).collect(
                        Collectors.toList());
            for (String fileNameToLoad : fileListCollected) {
                String fullPathToLogFile = Paths.get(databaseFolderPath).resolve(fileNameToLoad).toString();
                usageStatisticList.addAll(getAllStatistics(fullPathToLogFile));
            }
            return usageStatisticList;
        } else {
            throw new DatabaseFileNotFoundException("Database path '" + databaseFolderPath + "' is empty.");
        }
    }

    private List<UsageStatistic> loadStatisticFromDate(String startDate)
                throws IOException, DatabaseFileNotFoundException {
        List<UsageStatistic> usageStatisticList = new ArrayList<>();
        String[] fileList = Paths.get(databaseFolderPath).toFile().list();
        if (fileList != null) {
            Predicate<String> logFileFilter = filterLogFileFromDate(startDate);
            List<String> fileListCollected = Arrays.stream(fileList).filter(logFileFilter).collect(
                        Collectors.toList());
            for (String fileNameToLoad : fileListCollected) {
                String fullPathToLogFile = Paths.get(databaseFolderPath).resolve(fileNameToLoad).toString();
                usageStatisticList.addAll(getAllStatistics(fullPathToLogFile));
            }
            return usageStatisticList;
        } else {
            throw new DatabaseFileNotFoundException("Database path '" + databaseFolderPath + "' is empty.");
        }
    }

    private List<UsageStatistic> loadStatisticOfYearMonth(String year, String month)
                throws IOException, DatabaseFileNotFoundException {
        String firstDatetofYear = String.format("%s-%s-01", year, month);
        return loadStatisticOf(firstDatetofYear);
    }

    private Predicate<String> filterLogFileFromDate(final String startDate) {
        return aLong -> {
            Matcher matcher = groupMatchers(aLong);
            if (!matcher.find()) {
                return false;
            }
            String endDateFrmFileName = matcher.group(3);
            LocalDate end = LocalDate.parse(endDateFrmFileName);
            TemporalAccessor accessor = TIME_FORMATTER.parse(startDate);
            Date date = Date.from(Instant.from(accessor));
            LocalDate instant = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            return (end.isAfter(instant) || end.equals(instant));
        };
    }

    LocalDate getLastDayOfMonth(String dateTime) {
        return LocalDate.parse(dateTime).plusMonths(1).withDayOfMonth(DAY_OF_MONTH).minusDays(1);
    }

    LocalDate getFirstDayOfMonth(String dateTime) {
        return LocalDate.parse(dateTime).withDayOfMonth(DAY_OF_MONTH);
    }


    private List<UsageStatistic> getAllStatistics(String databasePathString) throws IOException {
        Gson gson = new Gson();
        List<UsageStatistic> usageStatisticList = new ArrayList<>();
        Path databasePath = Paths.get(databasePathString);
        LOGGER.info("retrieving usage statistics...");
        FileVisitor<Path> fileVisitor = new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                        throws IOException {
                LOGGER.info("checking file " + file);
                if (file.toString().toLowerCase().endsWith(".json") && !Files.isDirectory(file)) {
                    String fileContents = new String(Files.readAllBytes(file));
                    String reportingJsonString = constructJsonString(fileContents);
                    List<UsageStatistic> usageStatisticsSingleFile = gson.fromJson(reportingJsonString,
                                                                                   new TypeToken<List<UsageStatistic>>() {
                                                                                   }.getType());
                    usageStatisticList.addAll(usageStatisticsSingleFile);
                    return FileVisitResult.CONTINUE;
                } else {
                    return FileVisitResult.CONTINUE;
                }
            }
        };
        if (Files.isDirectory(databasePath)) {
            Files.walkFileTree(databasePath, fileVisitor);
        } else {
            LOGGER.info("checking file " + databasePath);
            String fileContents = new String(Files.readAllBytes(databasePath));
            String reportingJsonString = constructJsonString(fileContents);
            List<UsageStatistic> usageStatisticSingle = gson.fromJson(reportingJsonString,
                                                                      new TypeToken<List<UsageStatistic>>() {
                                                                      }.getType());
            usageStatisticList.addAll(usageStatisticSingle);
        }
        return usageStatisticList;
    }

    @NotNull
    private Matcher groupMatchers(String aLong) {
        Pattern compile = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})[_-]('*\\w*)[_-](\\d{4}-\\d{2}-\\d{2})");
        return compile.matcher(aLong);
    }

    private List<UsageStatistic> getSingleUserRangeStatistic(Predicate<Long> predTime, String userName,
                                                             List<UsageStatistic> allStatisticUsage) {
        ConcurrentHashMap<String, List<UsageStatistic>> filterUserWithDate = new ConcurrentHashMap<>();
        List<UsageStatistic> userStatisticInYear = new ArrayList<>();
        filterUserWithDate.put(userName, userStatisticInYear);
        allStatisticUsage.forEach(p -> filterUserWithDate.computeIfPresent(p.getUser(), (s, usageStatistics) -> {
            if (predTime.test(p.getFinishTime())) {
                userStatisticInYear.add(p);
            }
            return userStatisticInYear;
        }));
        return userStatisticInYear;
    }

    private List<UsageStatistic> getSingleUserDate(Predicate<FilterUserTimeInterval> intervalPredicate,
                                                   String user,
                                                   String year,
                                                   String month,
                                                   String day) throws IOException, DatabaseFileNotFoundException {

        final ConcurrentHashMap<String, List<UsageStatistic>> extractUserDate = new ConcurrentHashMap<>();
        final List<UsageStatistic> userStatisticInYear = new ArrayList<>();
        extractUserDate.put(user, userStatisticInYear);

        List<UsageStatistic> usageStatisticList = loadStatisticOfYearMonth(year, month);

        usageStatisticList
                    .forEach(usage ->
                                         extractUserDate.computeIfPresent(usage.getUser(),
                                                                          getUserFromYear(intervalPredicate, year,
                                                                                          month, day,
                                                                                          userStatisticInYear, usage)));
        return userStatisticInYear;
    }

    @NotNull
    private BiFunction<String, List<UsageStatistic>, List<UsageStatistic>> getUserFromYear(
                Predicate<FilterUserTimeInterval> intervalPredicate,
                String yr, String mnth, String dy,
                List<UsageStatistic> userStatisticInYear,
                UsageStatistic usage) {
        return (stringKey, usageStatistics) -> {
            FilterUserTimeInterval filterUserTimeInterval = new FilterUserTimeInterval(usage.getFinishTime(), yr, mnth,
                                                                                       dy);

            if (intervalPredicate.test(filterUserTimeInterval)) {
                userStatisticInYear.add(usage);
            }
            return userStatisticInYear;
        };
    }

    @NotNull
    private List<UsageStatistic> filterUser(String stringKey, List<UsageStatistic> usageStatisticList) {
        return usageStatisticList.stream().filter(p -> p.getUser().equalsIgnoreCase(stringKey)).collect(
                    Collectors.toList());
    }

    @NotNull
    private List<UsageStatistic> filterQueue(String queue, List<UsageStatistic> usageStatisticList) {
        return usageStatisticList.stream().filter(p -> p.getQueue().equalsIgnoreCase(queue)).collect(
                    Collectors.toList());
    }

    @NotNull
    private Predicate<Long> filterDateIntervals(final String startDate, final String endDate) {
        return aLong -> {
            Instant end = LocalDate.parse(endDate).atTime(LocalTime.MAX).toInstant(ZoneOffset.UTC);
            Instant start = LocalDate.parse(startDate).atStartOfDay().toInstant(ZoneOffset.UTC);
            Instant instant = new Date(aLong).toInstant();
            return instant.isAfter(start) && instant.isBefore(end);
        };
    }

    @NotNull
    private String constructJsonString(String fileContentsString) {
        String reportingJsonString = StringUtils.stripEnd(fileContentsString.trim(), ",");
        reportingJsonString = "[" + reportingJsonString + "]";
        return reportingJsonString;
    }

    static class FilterUserTimeInterval {

        private final String year;
        private final String month;
        private final String day;
        private Long finishTime;

        FilterUserTimeInterval(Long finishTime, String year, String month, String day) {
            this.finishTime = finishTime;
            this.year = year;
            this.month = month;
            this.day = day;
        }


        boolean filterMonth() {
            Instant start = Instant.parse(String.format("%s-%s-01T00:00:00.00Z", year, month));
            Instant end = Instant.parse(String.format("%s-0%s-01T00:00:00.00Z", year, Long.parseLong(month) + 1));
            Instant instant = new Date(finishTime).toInstant();
            return instant.isAfter(start) && instant.isBefore(end);

        }

        boolean filterDay() {
            Instant start = Instant.parse(String.format("%s-%s-%sT00:00:00.00Z", year, month, day));
            Instant end = Instant.parse(String.format("%s-%s-%sT23:59:00.00Z", year, month, day));
            Instant instant = new Date(finishTime).toInstant();
            return instant.isAfter(start) && instant.isBefore(end);
        }

        boolean filterYear() {
            Instant start = Instant.parse(String.format("%s-01-01T00:00:00.00Z", year));
            Instant end = Instant.parse(String.format("%d-01-01T00:00:00.00Z", Long.parseLong(year) + 1));
            Instant instant = new Date(finishTime).toInstant();
            return instant.isAfter(start) && instant.isBefore(end);
        }
    }

}
