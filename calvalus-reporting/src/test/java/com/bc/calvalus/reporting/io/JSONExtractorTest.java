package com.bc.calvalus.reporting.io;

import com.bc.calvalus.reporting.ws.UsageStatistic;
import com.bc.wps.utilities.PropertiesWrapper;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author hans
 */

@RunWith(LoadProperties.class)
public class JSONExtractorTest {

    private JSONExtractor jsonExtractor;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("conf/calvalus-reporting.properties");
        jsonExtractor = new JSONExtractor();
    }

    @Test
    public void canExtractAll() throws Exception {

        List<UsageStatistic> allStatistics = jsonExtractor.loadStatisticOf("2017-01-10");
        assertThat(allStatistics.size(), equalTo(3549));
        assertThat(allStatistics.get(0).getJobId(), equalTo("job_1481485063251_15808"));
        assertThat(allStatistics.get(0).getUser(), equalTo("martin"));
        assertThat(allStatistics.get(1).getJobId(), equalTo("job_1481485063251_15809"));
        assertThat(allStatistics.get(1).getUser(), equalTo("martin"));
        assertThat(allStatistics.get(2).getJobId(), equalTo("job_1481485063251_16187"));
        assertThat(allStatistics.get(2).getUser(), equalTo("martin"));
        assertThat(allStatistics.get(3).getJobId(), equalTo("job_1481485063251_16801"));
        assertThat(allStatistics.get(3).getUser(), equalTo("martin"));
    }

    @Test
    public void testSummaryOfAllUsers() throws Exception {
        Map<String, List<UsageStatistic>> allUserStatistic = jsonExtractor.getAllUserUsageStatistic("2017-01-10");
        Set<String> keySet = allUserStatistic.keySet();
        assertEquals(keySet.size(), 12);
    }

    @Test
    public void testTimeInterval() throws Exception {
        JSONExtractor.FilterUserTimeInterval timeInterval = new JSONExtractor.FilterUserTimeInterval(1483933291070L, "2017", "01", "01");
        boolean isWithIn = timeInterval.filterYear();
        assertTrue(isWithIn);
    }

    @Test
    public void testUSerStatisticInYear() throws Exception {
        List<UsageStatistic> usageStatisticList = jsonExtractor.getSingleUserUsageInYear("jessica", "2017");
        assertEquals(usageStatisticList.size(), 4);
        UsageStatistic usageStatistic = usageStatisticList.get(0);

        assertThat(usageStatistic.getJobId(), equalTo("job_1481485063251_20533"));
        assertThat(usageStatistic.getUser(), equalTo("jessica"));
        assertThat(usageStatistic.getQueue(), equalTo("default"));
        assertThat(usageStatistic.getStartTime(), equalTo(1484301451312L));
        assertThat(usageStatistic.getFinishTime(), equalTo(1484303865778L));
        assertThat(usageStatistic.getMapsCompleted(), equalTo(7));
        assertThat(usageStatistic.getReducesCompleted(), equalTo(0));
        assertThat(usageStatistic.getState(), equalTo("SUCCEEDED"));
        assertThat(usageStatistic.getInputPath(), equalTo("hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2017/01/13/000020/job_1481485063251_20533_conf.xml"));
        assertThat(usageStatistic.getFileBytesRead(), equalTo(0L));
        assertThat(usageStatistic.getFileBytesWritten(), equalTo(2100959L));
        assertThat(usageStatistic.getHdfsBytesRead(), equalTo(6972918194L));
        assertThat(usageStatistic.getHdfsBytesWritten(), equalTo(1776409134L));
        assertThat(usageStatistic.getvCoresMillisTotal(), equalTo(3255256L));
        assertThat(usageStatistic.getMbMillisMapTotal(), equalTo(8333455360L));
        assertThat(usageStatistic.getCpuMilliseconds(), equalTo(357650L));
    }

    @Test
    public void testUSerStatisticInYearMonth() throws Exception {
        JSONExtractor jsonExtractor = new JSONExtractor();
        List<UsageStatistic> usageStatisticList = jsonExtractor.getSingleUserUsageInYearMonth("thomas", "2017", "01");
        assertEquals(usageStatisticList.size(), 626);
        UsageStatistic usageStatistic = usageStatisticList.get(0);

        assertThat(usageStatistic.getJobId(), equalTo("job_1481485063251_18142"));
        assertThat(usageStatistic.getUser(), equalTo("thomas"));
        assertThat(usageStatistic.getQueue(), equalTo("high"));
        assertThat(usageStatistic.getStartTime(), equalTo(1484141759185L));
        assertThat(usageStatistic.getFinishTime(), equalTo(1484142199965L));
        assertThat(usageStatistic.getMapsCompleted(), equalTo(113));
        assertThat(usageStatistic.getReducesCompleted(), equalTo(0));
        assertThat(usageStatistic.getState(), equalTo("FAILED"));
        assertThat(usageStatistic.getInputPath(), equalTo("hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2017/01/11/000018/job_1481485063251_18142_conf.xml"));
        assertThat(usageStatistic.getFileBytesRead(), equalTo(0L));
        assertThat(usageStatistic.getFileBytesWritten(), equalTo(37940789L));
        assertThat(usageStatistic.getHdfsBytesRead(), equalTo(37483768696L));
        assertThat(usageStatistic.getHdfsBytesWritten(), equalTo(0L));
        assertThat(usageStatistic.getvCoresMillisTotal(), equalTo(0L));
        assertThat(usageStatistic.getMbMillisMapTotal(), equalTo(0L));
        assertThat(usageStatistic.getCpuMilliseconds(), equalTo(6757140L));
    }

    @Test
    public void testUSerStatisticInYearMonthDay() throws Exception {
        List<UsageStatistic> usageStatisticList = jsonExtractor.getSingleUserUsageYearMonthDay("martin", "2017", "01", "09");
        assertEquals(usageStatisticList.size(), 5);
        UsageStatistic usageStatistic = usageStatisticList.get(0);

        assertThat(usageStatistic.getJobId(), equalTo("job_1481485063251_15808"));
        assertThat(usageStatistic.getUser(), equalTo("martin"));
        assertThat(usageStatistic.getQueue(), equalTo("lc"));
        assertThat(usageStatistic.getStartTime(), equalTo(1483794176512L));
        assertThat(usageStatistic.getFinishTime(), equalTo(1483942744578L));
        assertThat(usageStatistic.getMapsCompleted(), equalTo(694));
        assertThat(usageStatistic.getReducesCompleted(), equalTo(0));
        assertThat(usageStatistic.getState(), equalTo("SUCCEEDED"));
        assertThat(usageStatistic.getInputPath(), equalTo("hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2017/01/09/000015/job_1481485063251_15808_conf.xml"));
        assertThat(usageStatistic.getFileBytesRead(), equalTo(0L));
        assertThat(usageStatistic.getFileBytesWritten(), equalTo(186692272L));
        assertThat(usageStatistic.getHdfsBytesRead(), equalTo(732875870L));
        assertThat(usageStatistic.getHdfsBytesWritten(), equalTo(1148345579L));
        assertThat(usageStatistic.getvCoresMillisTotal(), equalTo(148076365L));
        assertThat(usageStatistic.getMbMillisMapTotal(), equalTo(909781186560L));
        assertThat(usageStatistic.getCpuMilliseconds(), equalTo(29974960L));
    }

    @Ignore
    @Test
    public void testUserStatisticRange() throws Exception {
        List<UsageStatistic> usageStatisticList = jsonExtractor.getSingleUserUsageBetween("martin", "2017-01-09", "2017-01-15");
        assertEquals(usageStatisticList.size(), 1484);
    }

    @Test
    public void testAllDateUsageBetween() throws Exception {
        Map<String, List<UsageStatistic>> allUsersStartEndDateStatistic = jsonExtractor.getAllDateUsageBetween("2017-01-09", "2017-01-15");
        assertEquals(allUsersStartEndDateStatistic.size(), 7);
    }

    @Test
    public void testAllUserUsageBetween() throws Exception {
        Map<String, List<UsageStatistic>> allUsersStartEndDateStatistic = jsonExtractor.getAllUserUsageBetween("2017-01-01", "2017-01-30");
        List<UsageStatistic> usageStatisticList = allUsersStartEndDateStatistic.get("bla");
        assertEquals(usageStatisticList.size(), 2);
        assertEquals(allUsersStartEndDateStatistic.size(), 11);
    }

    @Test
    public void testAllQueueUsageBetween() throws Exception {
        Map<String, List<UsageStatistic>> allUsersStartEndDateStatistic = jsonExtractor.getAllQueueUsageBetween("2017-01-01", "2017-01-30");
        assertEquals(allUsersStartEndDateStatistic.size(), 10);
    }

    @Test
    public void testDateBetween() throws Exception {
        JSONExtractor jsonExtractor = new JSONExtractor();
        Set<String> datesBetween = jsonExtractor.getDatesBetween("2017-01-01", "2017-01-05");
        assertEquals(datesBetween.size(), 5);

        assertTrue(datesBetween.contains("2017-01-01"));
        assertTrue(datesBetween.contains("2017-01-02"));
        assertTrue(datesBetween.contains("2017-01-03"));
        assertTrue(datesBetween.contains("2017-01-04"));
        assertTrue(datesBetween.contains("2017-01-05"));
    }

    @Test
    public void testDateNotBetween() throws Exception {
        Set<String> datesBetween = jsonExtractor.getDatesBetween("2019-01-01", "2017-01-13");
        assertEquals(datesBetween.size(), 0);
    }


    @Test
    public void testSingleDateTimePredicate() throws Exception {
        Predicate<String> stringPredicate = jsonExtractor.filterFileLogBtwDate("2016-12-20");
        assertTrue(stringPredicate.test("2016-12-01_To_2016-12-31.json"));
        assertTrue(stringPredicate.test("2016-12-10_To_2016-12-31.json"));
        assertFalse(stringPredicate.test("2017-12-10_To_2016-12-31.json"));
    }

    @Test
    public void testBetweenDateTimePredicate() throws Exception {
        List<String> fileName = Arrays.asList(
                "calvalus-reporting-2014-12-01_to_2014-12-31.json",
                "calvalus-reporting-2015-03-01_To_2015-01-31.json",
                "calvalus-reporting-2016-11-01_To_2016-11-30.json",
                "calvalus-reporting-2016-03-01_To_2016-03-31.json",
                "calvalus-reporting-2017-09-01_To_2017-12-31.json",
                "calvalus-reporting-2016-12-01_To_2016-12-31.json",
                "calvalus-reporting-2018-06-01_To_2018-06-30.json",
                "calvalus-reporting-2019-07-01_To_2019-07-31.json",
                "calvalus-reporting-2022-12-01_To_2022-12-31.json");


        Predicate<String> stringPredicate = jsonExtractor.filterFileLogBtwDate("2016-12-02", "2016-12-12");
        List<String> collectFileBtwn = fileName.stream().filter(stringPredicate).collect(Collectors.toList());
        Assert.assertThat(collectFileBtwn.size(), is(1));


        Predicate<String> stringPredicateRange = jsonExtractor.filterFileLogBtwDate("2016-12-20", "2022-12-12");
        List<String> collectBtwn = fileName.stream().filter(stringPredicateRange).collect(Collectors.toList());
        Assert.assertThat(collectBtwn.size(), is(5));

    }

    @Test
    public void firstDayOfMonth() throws Exception {
        Assert.assertThat(jsonExtractor.getFirstDayOfMonth("2017-04-12").toString(), is("2017-04-01"));
        Assert.assertThat(jsonExtractor.getFirstDayOfMonth("2020-02-29").toString(), is("2020-02-01"));
        Assert.assertThat(jsonExtractor.getFirstDayOfMonth("2020-02-11").toString(), is("2020-02-01"));
    }

    @Test
    public void lastDayOfMonth() throws Exception {
        Assert.assertThat(jsonExtractor.getLastDayOfMonth("2014-03-12").toString(), is("2014-03-31"));
        Assert.assertThat(jsonExtractor.getLastDayOfMonth("2019-03-12").toString(), is("2019-03-31"));
        Assert.assertThat(jsonExtractor.getLastDayOfMonth("2020-12-12").toString(), is("2020-12-31"));
        Assert.assertThat(jsonExtractor.getLastDayOfMonth("2021-10-12").toString(), is("2021-10-31"));
    }

    @Test
    public void testDateNonExist() throws Exception {
        try {
            Assert.assertThat(jsonExtractor.getFirstDayOfMonth("2020/02/29T23:00").toString(), is("2020-02-01T00:00"));
            fail();
        } catch (DateTimeParseException e) {

        }
    }
}