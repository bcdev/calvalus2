package com.bc.calvalus.reporting.io;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.bc.calvalus.reporting.ws.NullUsageStatistic;
import com.bc.calvalus.reporting.ws.UsageStatistic;
import com.bc.wps.utilities.PropertiesWrapper;
import org.junit.*;

import java.util.List;

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

        assertThat(allStatistics.size(), equalTo(3546));
        assertThat(allStatistics.get(0).getJobId(), equalTo("job_1481485063251_15808"));
        assertThat(allStatistics.get(0).getUser(), equalTo("martin"));

        assertThat(allStatistics.get(1).getJobId(), equalTo("job_1481485063251_15809"));
        assertThat(allStatistics.get(1).getUser(), equalTo("martin"));

        assertThat(allStatistics.get(2).getJobId(), equalTo("job_1481485063251_16187"));
        assertThat(allStatistics.get(2).getUser(), equalTo("martin"));

        assertThat(allStatistics.get(3).getJobId(), equalTo("job_1481485063251_16801"));
        assertThat(allStatistics.get(3).getUser(), equalTo("martin"));
    }
}