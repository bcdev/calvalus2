package com.bc.calvalus.reporting.restservice.ws;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.*;

import java.util.Locale;

/**
 * @author hans
 */
public class ReportingServiceTest {

    private ReportingService reportingService;

    @Before
    public void setUp() throws Exception {
        Locale.setDefault(Locale.ENGLISH);
        reportingService = new ReportingService();
    }

    @Test
    public void canGetSingleJobReport() {
        String singleJobStatistic = reportingService.getSingleJobReport("job_1484837520075_13150", "2017-02-01");
        assertThat(singleJobStatistic, equalTo("{\n" +
                                               "  \"jobId\": \"job_1484837520075_13150\",\n" +
                                               "  \"user\": \"cvop\",\n" +
                                               "  \"queue\": \"consolidation\",\n" +
                                               "  \"submitTime\": 0,\n" +
                                               "  \"startTime\": 1486060805105,\n" +
                                               "  \"finishTime\": 1486061123937,\n" +
                                               "  \"state\": \"SUCCEEDED\",\n" +
                                               "  \"jobName\": \"ql S2A_MSIL1C_20170202T104241_N0204_R008_T32UMA_20170202T104536\",\n" +
                                               "  \"inputPath\": \"hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2017/02/02/000013/job_1484837520075_13150_conf.xml\",\n" +
                                               "  \"mapsCompleted\": 1,\n" +
                                               "  \"totalMaps\": 1,\n" +
                                               "  \"reducesCompleted\": 0,\n" +
                                               "  \"fileBytesRead\": 0,\n" +
                                               "  \"inputFileBytesRead\": 0,\n" +
                                               "  \"fileSplitBytesRead\": 0,\n" +
                                               "  \"fileBytesWritten\": 264225,\n" +
                                               "  \"hdfsBytesRead\": 315351229,\n" +
                                               "  \"hdfsBytesWritten\": 325321,\n" +
                                               "  \"mbMillisMapTotal\": 804208640,\n" +
                                               "  \"mbMillisReduceTotal\": 0,\n" +
                                               "  \"vCoresMillisTotal\": 314144,\n" +
                                               "  \"cpuMilliseconds\": 321370\n" +
                                               "}"));
    }

    @Test
    public void canGetAllJobReportsForUser() {
        String userJobStatistics = reportingService.getAllJobReportsForUser("user1");
        assertThat(userJobStatistics, equalTo("[\n" +
                                               "  {\n" +
                                               "    \"jobId\": \"job_1484837520075_13157\",\n" +
                                               "    \"user\": \"user1\",\n" +
                                               "    \"queue\": \"test\",\n" +
                                               "    \"submitTime\": 0,\n" +
                                               "    \"startTime\": 1486371600000,\n" +
                                               "    \"finishTime\": 1486061314131,\n" +
                                               "    \"state\": \"SUCCEEDED\",\n" +
                                               "    \"jobName\": \"S3 tar-to-zip 2017-02-02\",\n" +
                                               "    \"processType\": \"tar2zip\",\n" +
                                               "    \"inputPath\": \"hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2017/02/02/000013/job_1484837520075_13156_conf.xml\",\n" +
                                               "    \"mapsCompleted\": 1,\n" +
                                               "    \"totalMaps\": 1,\n" +
                                               "    \"reducesCompleted\": 0,\n" +
                                               "    \"fileBytesRead\": 0,\n" +
                                               "    \"inputFileBytesRead\": 0,\n" +
                                               "    \"fileSplitBytesRead\": 0,\n" +
                                               "    \"fileBytesWritten\": 265778,\n" +
                                               "    \"hdfsBytesRead\": 703253073,\n" +
                                               "    \"hdfsBytesWritten\": 701246583,\n" +
                                               "    \"mbMillisMapTotal\": 80919552,\n" +
                                               "    \"mbMillisReduceTotal\": 0,\n" +
                                               "    \"vCoresMillisTotal\": 79023,\n" +
                                               "    \"cpuMilliseconds\": 51550\n" +
                                               "  },\n" +
                                               "  {\n" +
                                               "    \"jobId\": \"job_1484837520075_13158\",\n" +
                                               "    \"user\": \"user1\",\n" +
                                               "    \"queue\": \"test\",\n" +
                                               "    \"submitTime\": 0,\n" +
                                               "    \"startTime\": 1486425600000,\n" +
                                               "    \"finishTime\": 1486458000000,\n" +
                                               "    \"state\": \"SUCCEEDED\",\n" +
                                               "    \"jobName\": \"S3 tar-to-zip 2017-02-02\",\n" +
                                               "    \"processType\": \"tar2zip\",\n" +
                                               "    \"inputPath\": \"hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2017/02/02/000013/job_1484837520075_13156_conf.xml\",\n" +
                                               "    \"mapsCompleted\": 1,\n" +
                                               "    \"totalMaps\": 1,\n" +
                                               "    \"reducesCompleted\": 0,\n" +
                                               "    \"fileBytesRead\": 0,\n" +
                                               "    \"inputFileBytesRead\": 0,\n" +
                                               "    \"fileSplitBytesRead\": 0,\n" +
                                               "    \"fileBytesWritten\": 265778,\n" +
                                               "    \"hdfsBytesRead\": 703253073,\n" +
                                               "    \"hdfsBytesWritten\": 701246583,\n" +
                                               "    \"mbMillisMapTotal\": 80919552,\n" +
                                               "    \"mbMillisReduceTotal\": 0,\n" +
                                               "    \"vCoresMillisTotal\": 79023,\n" +
                                               "    \"cpuMilliseconds\": 51550\n" +
                                               "  }\n" +
                                               "]"));
    }

    @Test
    public void canGetUserAggregatedJobReports() {
        String userAggregatedStatistics = reportingService.getUserAggregatedJobReports();
        assertThat(userAggregatedStatistics, equalTo("[\n" +
                                                     "  {\n" +
                                                     "    \"memoryUsagePrice\": \"9.787222222222221E-6\",\n" +
                                                     "    \"totalFileReadingMb\": \"1,340\",\n" +
                                                     "    \"cpuUsagePrice\": \"0.0\",\n" +
                                                     "    \"diskUsageprice\": \"0.0224\",\n" +
                                                     "    \"totalPrice\": \"0.022409787222222224\",\n" +
                                                     "    \"totalFileWritingMb\": \"1,338\",\n" +
                                                     "    \"totalMemoryUsedMbs\": \"161,838\",\n" +
                                                     "    \"totalCpuTimeSpent\": \"00:01:43\",\n" +
                                                     "    \"jobsProcessed\": \"2\",\n" +
                                                     "    \"user\": \"user1\",\n" +
                                                     "    \"totalVcoresUsed\": \"158\",\n" +
                                                     "    \"totalMap\": \"2\"\n" +
                                                     "  },\n" +
                                                     "  {\n" +
                                                     "    \"memoryUsagePrice\": \"4.875027777777778E-5\",\n" +
                                                     "    \"totalFileReadingMb\": \"0\",\n" +
                                                     "    \"cpuUsagePrice\": \"0.0\",\n" +
                                                     "    \"diskUsageprice\": \"0.0\",\n" +
                                                     "    \"totalPrice\": \"4.875027777777778E-5\",\n" +
                                                     "    \"totalFileWritingMb\": \"8\",\n" +
                                                     "    \"totalMemoryUsedMbs\": \"806,758\",\n" +
                                                     "    \"totalCpuTimeSpent\": \"00:00:06\",\n" +
                                                     "    \"jobsProcessed\": \"2\",\n" +
                                                     "    \"user\": \"martin\",\n" +
                                                     "    \"totalVcoresUsed\": \"0\",\n" +
                                                     "    \"totalMap\": \"0\"\n" +
                                                     "  },\n" +
                                                     "  {\n" +
                                                     "    \"memoryUsagePrice\": \"1.265525E-4\",\n" +
                                                     "    \"totalFileReadingMb\": \"4,267\",\n" +
                                                     "    \"cpuUsagePrice\": \"0.0\",\n" +
                                                     "    \"diskUsageprice\": \"0.056\",\n" +
                                                     "    \"totalPrice\": \"0.0561265525\",\n" +
                                                     "    \"totalFileWritingMb\": \"994\",\n" +
                                                     "    \"totalMemoryUsedMbs\": \"2,092,547\",\n" +
                                                     "    \"totalCpuTimeSpent\": \"00:13:44\",\n" +
                                                     "    \"jobsProcessed\": \"9\",\n" +
                                                     "    \"user\": \"cvop\",\n" +
                                                     "    \"totalVcoresUsed\": \"1,011\",\n" +
                                                     "    \"totalMap\": \"9\"\n" +
                                                     "  }\n" +
                                                     "]"));
    }

    @Test
    public void canGetFromDate() {
        String usageStatistics = reportingService.getFromDate("2017-06-01T00:01:42Z");
        assertThat(usageStatistics, equalTo("[\n" +
                                            "  {\n" +
                                            "    \"jobId\": \"job_1495452880837_5073\",\n" +
                                            "    \"user\": \"cvop\",\n" +
                                            "    \"queue\": \"consolidation\",\n" +
                                            "    \"submitTime\": 0,\n" +
                                            "    \"startTime\": 1496275056280,\n" +
                                            "    \"finishTime\": 1496275302818,\n" +
                                            "    \"state\": \"SUCCEEDED\",\n" +
                                            "    \"jobName\": \"ql S2A_MSIL1C_20170530T103021_N0205_R108_T32UNA_20170530T103024\",\n" +
                                            "    \"workflowType\": \"QL\",\n" +
                                            "    \"inputPath\": \"hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2017/06/01/000005/job_1495452880837_5073_conf.xml\",\n" +
                                            "    \"inProductType\": \"/calvalus/eodata/S2_L1C/v2/2017/05/30/S2A_MSIL1C_20170530T103021_N0205_R108_T32UNA_20170530T103024.zip\",\n" +
                                            "    \"mapsCompleted\": 1,\n" +
                                            "    \"totalMaps\": 1,\n" +
                                            "    \"reducesCompleted\": 0,\n" +
                                            "    \"fileBytesRead\": 0,\n" +
                                            "    \"inputFileBytesRead\": 0,\n" +
                                            "    \"fileSplitBytesRead\": 0,\n" +
                                            "    \"fileBytesWritten\": 274389,\n" +
                                            "    \"hdfsBytesRead\": 809763005,\n" +
                                            "    \"hdfsBytesWritten\": 548207,\n" +
                                            "    \"mbMillisMapTotal\": 622668800,\n" +
                                            "    \"mbMillisReduceTotal\": 0,\n" +
                                            "    \"vCoresMillisTotal\": 243230,\n" +
                                            "    \"cpuMilliseconds\": 250370\n" +
                                            "  }\n" +
                                            "]"));
    }
}