package com.bc.calvalus.reporting.code;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.bc.calvalus.reporting.common.UsageStatistic;
import org.junit.*;

/**
 * @author hans
 */
public class CodeReportTest {

    @Test
    public void canParseCorrectly() {
        UsageStatistic usageStatistic = new UsageStatistic("job-01",
                                                           "user",
                                                           "queue",
                                                           1514764800000L,
                                                           1514768400000L,
                                                           1514851200000L,
                                                           "SUCCESSFUL",
                                                           "Job 01",
                                                           "remoteUser",
                                                           "remoteRef",
                                                           "Fmask",
                                                           "L2",
                                                           "/path/to/output",
                                                           "/path/to/input",
                                                           "S2.L1C",
                                                           "2",
                                                           "4096",
                                                           "BC Calvalus",
                                                           "S2_MASKED",
                                                           "file:/calvalus/home/user/12345",
                                                           5,
                                                           5,
                                                           0,
                                                           131298L,
                                                           0L,
                                                           131298131298L,
                                                           824404532L,
                                                           1159308L,
                                                           1159308L,
                                                           4748525568L,
                                                           0L,
                                                           1159308L,
                                                           59360L);
        CodeReport codeReport = new CodeReport(usageStatistic);
        assertThat(codeReport.toJson(), containsString("{\n" +
                                                       "  \"requestId\": \"job-01\",\n" +
                                                       "  \"jobName\": \"Job 01\",\n" +
                                                       "  \"jobSubmissionTime\": \"2018-01-01T01:00:00.000Z\",\n" +
                                                       "  \"userName\": \"user\",\n" +
                                                       "  \"inProducts\": \"/path/to/output\",\n" +
                                                       "  \"inProductsType\": \"/path/to/input\",\n" +
                                                       "  \"inCollection\": \"S2.L1C\",\n" +
                                                       "  \"inProductsNumber\": 5,\n" +
                                                       "  \"inProductsSize\": 122.281036,\n" +
                                                       "  \"requestSource\": \"BC Calvalus\",\n" +
                                                       "  \"processingCenter\": \"Calvalus\",\n" +
                                                       "  \"configuredCpuCoresPerTask\": 2,\n" +
                                                       "  \"cpuCoreHours\": 0.016489,\n" +
                                                       "  \"processorName\": \"Fmask\",\n" +
                                                       "  \"configuredRamPerTask\": 4.000000,\n" +
                                                       "  \"ramHours\": 1.288120,\n" +
                                                       "  \"processingWorkflow\": \"L2\",\n" +
                                                       "  \"duration\": 82800.000000,\n" +
                                                       "  \"processingStatus\": \"SUCCESSFUL\",\n" +
                                                       "  \"outProductsNumber\": 5,\n" +
                                                       "  \"outProductsType\": \"S2_MASKED\",\n" +
                                                       "  \"outCollection\": \"Job 01\",\n" +
                                                       "  \"outProductsLocation\": \"file:/calvalus/home/user/12345\",\n" +
                                                       "  \"outProductsSize\": 0.767787,\n" +
                                                       "  \"messageType\": \"ProductProcessedMessage\",\n" +
                                                       "  \"serviceId\": \"code-de-processing-service\",\n"));
        assertThat(codeReport.toJson(), containsString("  \"version\": \"1.0\"\n" +
                                                       "}"));
    }

    @Test
    public void canDeserialize() {
        CodeReport codeReport = new CodeReport("id",
                                               "job",
                                               "jobSubmissionTime",
                                               "userName",
                                               "inProducts",
                                               "inProductsType",
                                               "inCollection",
                                               0,
                                               0.123456789,
                                               "requestSource",
                                               "processingCenter",
                                               0,
                                               0.123456789,
                                               "processorName",
                                               0.123456789,
                                               0.123456789,
                                               "processingWorkflow",
                                               0.123456789,
                                               "processingStatus",
                                               0,
                                               "outProductsType",
                                               "outCollection",
                                               "outProductsLocation",
                                               0.123456789
        );
        assertThat(codeReport.toJson(), containsString("{\n" +
                                                       "  \"requestId\": \"id\",\n" +
                                                       "  \"jobName\": \"job\",\n" +
                                                       "  \"jobSubmissionTime\": \"jobSubmissionTime\",\n" +
                                                       "  \"userName\": \"userName\",\n" +
                                                       "  \"inProducts\": \"inProducts\",\n" +
                                                       "  \"inProductsType\": \"inProductsType\",\n" +
                                                       "  \"inCollection\": \"inCollection\",\n" +
                                                       "  \"inProductsNumber\": 0,\n" +
                                                       "  \"inProductsSize\": 0.123457,\n" +
                                                       "  \"requestSource\": \"requestSource\",\n" +
                                                       "  \"processingCenter\": \"processingCenter\",\n" +
                                                       "  \"configuredCpuCoresPerTask\": 0,\n" +
                                                       "  \"cpuCoreHours\": 0.123457,\n" +
                                                       "  \"processorName\": \"processorName\",\n" +
                                                       "  \"configuredRamPerTask\": 0.123457,\n" +
                                                       "  \"ramHours\": 0.123457,\n" +
                                                       "  \"processingWorkflow\": \"processingWorkflow\",\n" +
                                                       "  \"duration\": 0.123457,\n" +
                                                       "  \"processingStatus\": \"processingStatus\",\n" +
                                                       "  \"outProductsNumber\": 0,\n" +
                                                       "  \"outProductsType\": \"outProductsType\",\n" +
                                                       "  \"outCollection\": \"outCollection\",\n" +
                                                       "  \"outProductsLocation\": \"outProductsLocation\",\n" +
                                                       "  \"outProductsSize\": 0.123457,\n" +
                                                       "  \"messageType\": \"ProductProcessedMessage\",\n" +
                                                       "  \"serviceId\": \"code-de-processing-service\","));
        assertThat(codeReport.toJson(), containsString("  \"version\": \"1.0\"\n" +
                                                       "}"));
    }
}