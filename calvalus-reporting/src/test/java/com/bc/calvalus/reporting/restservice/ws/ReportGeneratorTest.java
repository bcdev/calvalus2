package com.bc.calvalus.reporting.restservice.ws;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

import com.bc.calvalus.reporting.common.UsageStatistic;
import com.bc.calvalus.reporting.restservice.io.JSONExtractor;
import com.bc.wps.utilities.PropertiesWrapper;
import org.junit.*;

import java.util.List;
import java.util.Map;

/**
 * @author hans
 */
@Ignore("ignored because a new change in JSONExtractor broke the loading data mechanism (database path hardcoded to local personal dir)")
public class ReportGeneratorTest {

    private ReportGenerator reportGenerator;

    private JSONExtractor jsonExtractor;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("conf/calvalus-reporting.properties");
        jsonExtractor = new JSONExtractor();
    }

    @Test
    public void canGenerateTextSingleJob() throws Exception {
        UsageStatistic usageStatistic = jsonExtractor.getSingleStatistic("job_1481485063251_20052", "2017-01-10");
        reportGenerator = new ReportGenerator();

        assertThat(reportGenerator.generateTextSingleJob(usageStatistic), equalTo("Usage statistic for job 'job_1481485063251_20052'\n" +
                                                                                          "\n" +
                                                                                          "Project : fire\n" +
                                                                                          "Start time : 12.01.2017 17:03:40\n" +
                                                                                          "Finish time : 12.01.2017 17:33:06\n" +
                                                                                          "Total time : 00:29:26\nStatus :  SUCCEEDED\n" +
                                                                                          "Total file writing (MB) : 89\n" +
                                                                                          "Total file reading (MB) : 262\n" +
                                                                                          "Total CPU time spent : 00:29:58\n" +
                                                                                          "Total Memory used (MB s) :  7,500,170\n" +
                                                                                          "Total vCores used (vCore s) :  1,762\n"));
    }

    @Test
    public void canGenerateJsonSingleJob() throws Exception {
        UsageStatistic usageStatistic = jsonExtractor.getSingleStatistic("job_1481485063251_20052","2017-01-10");
        reportGenerator = new ReportGenerator();

        assertThat(reportGenerator.generateJsonSingleJob(usageStatistic), equalTo("{\n" +
                                                                                          "  \"jobId\": \"job_1481485063251_20052\",\n" +
                                                                                          "  \"finishTime\": \"12.01.2017 17:33:06\",\n" +
                                                                                          "  \"totalFileReadingMb\": \"262\",\n" +
                                                                                          "  \"totalCpuTime\": \"00:29:58\",\n" +
                                                                                          "  \"totalTime\": \"00:29:26\",\n" +
                                                                                          "  \"totalFileWritingMb\": \"89\",\n" +
                                                                                          "  \"totalMemoryUsedMbs\": \"7,500,170\",\n" +
                                                                                          "  \"project\": \"fire\",\n" +
                                                                                          "  \"startTime\": \"12.01.2017 17:03:40\",\n" +
                                                                                          "  \"totalVcoresUsed\": \"1,762\",\n" +
                                                                                          "  \"status\": \"SUCCEEDED\"\n" +
                                                                                          "}"));
    }

    @Test
    public void testGenerateAllUserJobSummary() throws Exception {
        reportGenerator = new ReportGenerator();
        Map<String, List<UsageStatistic>> allUserStatistic = jsonExtractor.getAllUserUsageStatistic("2017-01-10");
        String jsonAllUserJobSummary = reportGenerator.generateJsonAllUserJobSummary(allUserStatistic);
        assertNotNull(jsonAllUserJobSummary);
    }

    @Test
    public void testGenerateAllDateJobSummaryBetween() throws Exception {
        reportGenerator = new ReportGenerator();
        Map<String, List<UsageStatistic>> allUserStatistic = jsonExtractor.getAllDateUsageBetween("2017-01-01", "2017-01-12");
        String jsonAllUserJobSummary = reportGenerator.generateJsonUsageBetween(allUserStatistic, "jobsInDate");
        assertNotNull(jsonAllUserJobSummary);
    }

    @Test
    public void testGenerateAllUserJobSummaryBetween() throws Exception {
        reportGenerator = new ReportGenerator();
        Map<String, List<UsageStatistic>> allUserStatistic = jsonExtractor.getAllUserUsageBetween("2017-01-01", "2017-01-30");
        String jsonAllUserJobSummary = reportGenerator.generateJsonAllUserJobSummary(allUserStatistic);
        assertNotNull(jsonAllUserJobSummary);
        //todo mba*** add more assertion
    }

    @Test
    public void testGenerateAllQueueJobSummaryBetween() throws Exception {
        reportGenerator = new ReportGenerator();
        Map<String, List<UsageStatistic>> allUserStatistic = jsonExtractor.getAllQueueUsageBetween("2017-01-01", "2017-01-30");
        String queue = reportGenerator.generateJsonUsageBetween(allUserStatistic, "jobsInQueue");
        assertNotNull(queue);
        //todo mba*** add more assertion
    }

    @Test
    public void canGenerateSingleUserJson() throws Exception {
        List<UsageStatistic> usageStatistics = jsonExtractor.getSingleUserStatistic("cvop", "2017-01-20");

        reportGenerator = new ReportGenerator();

        assertThat(reportGenerator.generateJsonUserSingleJob(usageStatistics), equalTo("{\n" +
                                                                                               "  \"memoryUsagePrice\": \"1.78\",\n" +
                                                                                               "  \"totalFileReadingMb\": \"37,826,596\",\n" +
                                                                                               "  \"cpuUsagePrice\": \"10.33\",\n" +
                                                                                               "  \"diskUsageprice\": \"415.15\",\n" +
                                                                                               "  \"totalPrice\": \"427.26\",\n" +
                                                                                               "  \"totalFileWritingMb\": \"130,388\",\n" +
                                                                                               "  \"totalMemoryUsedMbs\": \"29,343,671,531\",\n" +
                                                                                               "  \"totalCpuTimeSpent\": \"6426:17:03\",\n" +
                                                                                               "  \"jobsProcessed\": \"1381\",\n" +
                                                                                               "  \"user\": \"cvop\",\n" +
                                                                                               "  \"totalVcoresUsed\": \"28,138,453\",\n" +
                                                                                               "  \"totalMap\": \"0\"\n" +
                                                                                               "}"));

    }

    @Test
    public void canGenerateSingleUserYearJson() throws Exception {
        List<UsageStatistic> usageStatistics = jsonExtractor.getSingleUserUsageInYear("cvop", "2017");

        reportGenerator = new ReportGenerator();

        assertThat(reportGenerator.generateJsonUserSingleJob(usageStatistics), equalTo("{\n" +
                                                                                               "  \"memoryUsagePrice\": \"3.55\",\n" +
                                                                                               "  \"totalFileReadingMb\": \"75,653,192\",\n" +
                                                                                               "  \"cpuUsagePrice\": \"20.67\",\n" +
                                                                                               "  \"diskUsageprice\": \"830.3\",\n" +
                                                                                               "  \"totalPrice\": \"854.52\",\n" +
                                                                                               "  \"totalFileWritingMb\": \"260,776\",\n" +
                                                                                               "  \"totalMemoryUsedMbs\": \"58,687,343,062\",\n" +
                                                                                               "  \"totalCpuTimeSpent\": \"12852:34:07\",\n" +
                                                                                               "  \"jobsProcessed\": \"2762\",\n" +
                                                                                               "  \"user\": \"cvop\",\n" +
                                                                                               "  \"totalVcoresUsed\": \"56,276,906\",\n" +
                                                                                               "  \"totalMap\": \"0\"\n" +
                                                                                               "}"));
    }
}