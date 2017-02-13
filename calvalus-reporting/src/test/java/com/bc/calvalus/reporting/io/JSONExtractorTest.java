package com.bc.calvalus.reporting.io;

import com.bc.calvalus.reporting.ws.NullUsageStatistic;
import com.bc.calvalus.reporting.ws.UsageStatistic;
import com.bc.wps.utilities.PropertiesWrapper;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author hans
 */
public class JSONExtractorTest {

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("conf/calvalus-reporting.properties");
    }

    @Test
    public void canExtractSingleJob() throws Exception {
        JSONExtractor jsonExtractor = new JSONExtractor();

        UsageStatistic usageStatistic = jsonExtractor.getSingleStatistic("job_1484434434570_0373");

        assertThat(usageStatistic.getJobId(), equalTo("job_1484434434570_0373"));
        assertThat(usageStatistic.getUser(), equalTo("cvop"));
        assertThat(usageStatistic.getQueue(), equalTo("nrt"));
        assertThat(usageStatistic.getStartTime(), equalTo(1484563112510L));
        assertThat(usageStatistic.getFinishTime(), equalTo(1484563135636L));
        assertThat(usageStatistic.getMapsCompleted(), equalTo(1));
        assertThat(usageStatistic.getReducesCompleted(), equalTo(0));
        assertThat(usageStatistic.getState(), equalTo("SUCCEEDED"));
        assertThat(usageStatistic.getInputPath(), equalTo("hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2017/01/16/000000/job_1484434434570_0373_conf.xml"));
        assertThat(usageStatistic.getFileBytesRead(), equalTo(0L));
        assertThat(usageStatistic.getFileBytesWritten(), equalTo(266969L));
        assertThat(usageStatistic.getHdfsBytesRead(), equalTo(2989591L));
        assertThat(usageStatistic.getHdfsBytesWritten(), equalTo(58939L));
        assertThat(usageStatistic.getvCoresMillisTotal(), equalTo(18385L));
        assertThat(usageStatistic.getMbMillisTotal(), equalTo(18826240L));
        assertThat(usageStatistic.getCpuMilliseconds(), equalTo(23240L));
    }

    @Test
    public void canReturnNullObjectWhenJobNotFound() throws Exception {
        JSONExtractor jsonExtractor = new JSONExtractor();

        UsageStatistic usageStatistic = jsonExtractor.getSingleStatistic("");

        assertThat(usageStatistic, instanceOf(NullUsageStatistic.class));
    }

    @Test
    public void canExtractAll() throws Exception {
        JSONExtractor jsonExtractor = new JSONExtractor();
        List<UsageStatistic> allStatistics = jsonExtractor.getAllStatistics();

        assertThat(allStatistics.size(), equalTo(3547));
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
        JSONExtractor jsonExtractor = new JSONExtractor();
        Map<String, List<UsageStatistic>> allUserStatistic = jsonExtractor.getAllUserUsageStatistic();
        Set<String> keySet = allUserStatistic.keySet();
        assertEquals(keySet.size(), 11);
    }

    @Test
    public void testTimeInterval() throws Exception {
        JSONExtractor.FilterUserTimeInterval timeInterval = new JSONExtractor.FilterUserTimeInterval(1483933291070L, "2017", "01", "01");
        boolean isWithIn = timeInterval.filterYear();
        assertTrue(isWithIn);
    }

    @Test
    public void testUSerStatisticInYear() throws Exception {
        JSONExtractor jsonExtractor = new JSONExtractor();
        List<UsageStatistic> usageStatisticList = jsonExtractor.getSingleUserUsageInYear("jessica", "2017");
        assertEquals(usageStatisticList.size(), 2);
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
        assertThat(usageStatistic.getMbMillisTotal(), equalTo(8333455360L));
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
        assertThat(usageStatistic.getMbMillisTotal(), equalTo(0L));
        assertThat(usageStatistic.getCpuMilliseconds(), equalTo(6757140L));
    }

    @Test
    public void testUSerStatisticInYearMonthDay() throws Exception {
        JSONExtractor jsonExtractor = new JSONExtractor();
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
        assertThat(usageStatistic.getMbMillisTotal(), equalTo(909781186560L));
        assertThat(usageStatistic.getCpuMilliseconds(), equalTo(29974960L));
    }

    @Ignore
    @Test
    public void testUserStatisticRange() throws Exception {
        JSONExtractor jsonExtractor = new JSONExtractor();
        List<UsageStatistic> usageStatisticList = jsonExtractor.getSingleUserUsageBetween("martin", "2017-01-09", "2017-01-15");
        assertEquals(usageStatisticList.size(), 1484);
    }

    @Test
    public void testAllUserStatisticRangeGroupByDate() throws Exception {
        JSONExtractor jsonExtractor = new JSONExtractor();
        Map<String, List<UsageStatistic>> allUsersStartEndDateStatistic = jsonExtractor.getAllUsageBetween("2017-01-09", "2017-01-15");
        assertEquals(allUsersStartEndDateStatistic.size(), 7);
    }

    @Test
    public void testDateBetween() throws Exception {
        JSONExtractor jsonExtractor = new JSONExtractor();
        Set<String> datesBetween = jsonExtractor.getDatesBetween("2017-01-01", "2017-01-13");
        assertEquals(datesBetween.size(), 13);

        assertTrue(datesBetween.contains("2017-01-01"));
        assertTrue(datesBetween.contains("2017-01-02"));
        assertTrue(datesBetween.contains("2017-01-03"));
        assertTrue(datesBetween.contains("2017-01-04"));
        assertTrue(datesBetween.contains("2017-01-05"));
        assertTrue(datesBetween.contains("2017-01-06"));
        assertTrue(datesBetween.contains("2017-01-07"));
        assertTrue(datesBetween.contains("2017-01-08"));
        assertTrue(datesBetween.contains("2017-01-09"));
        assertTrue(datesBetween.contains("2017-01-10"));
        assertTrue(datesBetween.contains("2017-01-11"));
        assertTrue(datesBetween.contains("2017-01-13"));
    }

    @Test
    public void testDateNotBetween() throws Exception {
        JSONExtractor jsonExtractor = new JSONExtractor();
        Set<String> datesBetween = jsonExtractor.getDatesBetween("2019-01-01", "2017-01-13");
        assertEquals(datesBetween.size(), 0);
    }
}