package com.bc.calvalus.reporting.io;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

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
    public void canExtractAll() throws Exception {
        JSONExtractor jsonExtractor = new JSONExtractor();
        List<UsageStatistic> allStatistics = jsonExtractor.getAllStatistics();

        assertThat(allStatistics.size(), equalTo(4));
        assertThat(allStatistics.get(0).getJobId(), equalTo("job_1481485063251_6300"));
        assertThat(allStatistics.get(0).getUser(), equalTo("user1"));

        assertThat(allStatistics.get(1).getJobId(), equalTo("job_1481485063251_8803"));
        assertThat(allStatistics.get(1).getUser(), equalTo("user1"));

        assertThat(allStatistics.get(2).getJobId(), equalTo("job_1481485063251_9981"));
        assertThat(allStatistics.get(2).getUser(), equalTo("user2"));

        assertThat(allStatistics.get(3).getJobId(), equalTo("job_1481485063251_9846"));
        assertThat(allStatistics.get(3).getUser(), equalTo("user2"));
    }
}