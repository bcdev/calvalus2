package com.bc.calvalus.reporting.code;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.bc.calvalus.reporting.common.UsageStatistic;
import com.bc.wps.utilities.PropertiesWrapper;
import org.junit.*;

/**
 * @author hans
 */
public class CodeReportTest {

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("code-de.properties");
    }

    @Test
    public void canDeserializeCorrectly() {
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
                                                           "Fmask for Landsat 8 and Sentinel 2 v0.1.0",
                                                           "com.bc.calvalus.processing.l2.L2FormattingMapper",
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
        assertThat(codeReport.toJson(PropertiesWrapper.get("mandatory.fields")), containsString("{\n" +
                                                       "  \"requestId\": \"job-01\",\n" +
                                                       "  \"jobName\": \"Job 01\",\n" +
                                                       "  \"jobSubmissionTime\": \"2018-01-01T01:00:00.000Z\",\n" +
                                                       "  \"userName\": \"user\",\n" +
                                                       "  \"queueName\": \"queue\",\n" +
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
                                              "  \"processorDescription\": \"Fmask for Landsat 8 and Sentinel 2 v0.1.0\",\n" +
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
        assertThat(codeReport.toJson(PropertiesWrapper.get("mandatory.fields")), containsString("  \"version\": \"1.0\"\n" +
                                                       "}"));
    }

    @Test
    public void canDeserializeL2ProcessFormatting() {
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
                                                           null,
                                                           null,
                                                           "com.bc.calvalus.processing.l2.L2FormattingMapper",
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
        assertThat(codeReport.toJson(PropertiesWrapper.get("mandatory.fields")), containsString("{\n" +
                                                       "  \"requestId\": \"job-01\",\n" +
                                                       "  \"jobName\": \"Job 01\",\n" +
                                                       "  \"jobSubmissionTime\": \"2018-01-01T01:00:00.000Z\",\n" +
                                                       "  \"userName\": \"user\",\n" +
                                                       "  \"queueName\": \"queue\",\n" +
                                                       "  \"inProducts\": \"/path/to/output\",\n" +
                                                       "  \"inProductsType\": \"/path/to/input\",\n" +
                                                       "  \"inCollection\": \"S2.L1C\",\n" +
                                                       "  \"inProductsNumber\": 5,\n" +
                                                       "  \"inProductsSize\": 122.281036,\n" +
                                                       "  \"requestSource\": \"BC Calvalus\",\n" +
                                                       "  \"processingCenter\": \"Calvalus\",\n" +
                                                       "  \"configuredCpuCoresPerTask\": 2,\n" +
                                                       "  \"cpuCoreHours\": 0.016489,\n" +
                                                       "  \"processorName\": \"Formatting\",\n" +
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
        assertThat(codeReport.toJson(PropertiesWrapper.get("mandatory.fields")), containsString("  \"version\": \"1.0\"\n" +
                                                       "}"));
    }

    @Test
    public void canDeserializeL2ProcessNoProcessor() {
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
                                                           null,
                                                           null,
                                                           "com.bc.calvalus.processing.l2.SomethingElse",
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
        assertThat(codeReport.toJson(PropertiesWrapper.get("mandatory.fields")), containsString("{\n" +
                                                       "  \"requestId\": \"job-01\",\n" +
                                                       "  \"jobName\": \"Job 01\",\n" +
                                                       "  \"jobSubmissionTime\": \"2018-01-01T01:00:00.000Z\",\n" +
                                                       "  \"userName\": \"user\",\n" +
                                                       "  \"queueName\": \"queue\",\n" +
                                                       "  \"inProducts\": \"/path/to/output\",\n" +
                                                       "  \"inProductsType\": \"/path/to/input\",\n" +
                                                       "  \"inCollection\": \"S2.L1C\",\n" +
                                                       "  \"inProductsNumber\": 5,\n" +
                                                       "  \"inProductsSize\": 122.281036,\n" +
                                                       "  \"requestSource\": \"BC Calvalus\",\n" +
                                                       "  \"processingCenter\": \"Calvalus\",\n" +
                                                       "  \"configuredCpuCoresPerTask\": 2,\n" +
                                                       "  \"cpuCoreHours\": 0.016489,\n" +
                                                       "  \"processorName\": \"None\",\n" +
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
        assertThat(codeReport.toJson(PropertiesWrapper.get("mandatory.fields")), containsString("  \"version\": \"1.0\"\n" +
                                                       "}"));
    }

    @Test
    public void canDeserializeL3ProcessFormatting() {
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
                                                           null,
                                                           null,
                                                           "com.bc.calvalus.processing.l3.L3FormatterMapper",
                                                           "L3",
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
        assertThat(codeReport.toJson(PropertiesWrapper.get("mandatory.fields")), containsString("{\n" +
                                                       "  \"requestId\": \"job-01\",\n" +
                                                       "  \"jobName\": \"Job 01\",\n" +
                                                       "  \"jobSubmissionTime\": \"2018-01-01T01:00:00.000Z\",\n" +
                                                       "  \"userName\": \"user\",\n" +
                                                       "  \"queueName\": \"queue\",\n" +
                                                       "  \"inProducts\": \"/path/to/output\",\n" +
                                                       "  \"inProductsType\": \"/path/to/input\",\n" +
                                                       "  \"inCollection\": \"S2.L1C\",\n" +
                                                       "  \"inProductsNumber\": 5,\n" +
                                                       "  \"inProductsSize\": 122.281036,\n" +
                                                       "  \"requestSource\": \"BC Calvalus\",\n" +
                                                       "  \"processingCenter\": \"Calvalus\",\n" +
                                                       "  \"configuredCpuCoresPerTask\": 2,\n" +
                                                       "  \"cpuCoreHours\": 0.016489,\n" +
                                                       "  \"processorName\": \"Formatting\",\n" +
                                                       "  \"configuredRamPerTask\": 4.000000,\n" +
                                                       "  \"ramHours\": 1.288120,\n" +
                                                       "  \"processingWorkflow\": \"L3\",\n" +
                                                       "  \"duration\": 82800.000000,\n" +
                                                       "  \"processingStatus\": \"SUCCESSFUL\",\n" +
                                                       "  \"outProductsNumber\": 5,\n" +
                                                       "  \"outProductsType\": \"S2_MASKED\",\n" +
                                                       "  \"outCollection\": \"Job 01\",\n" +
                                                       "  \"outProductsLocation\": \"file:/calvalus/home/user/12345\",\n" +
                                                       "  \"outProductsSize\": 0.767787,\n" +
                                                       "  \"messageType\": \"ProductProcessedMessage\",\n" +
                                                       "  \"serviceId\": \"code-de-processing-service\",\n"));
        assertThat(codeReport.toJson(PropertiesWrapper.get("mandatory.fields")), containsString("  \"version\": \"1.0\"\n" +
                                                       "}"));
    }

    @Test
    public void canDeserializeL3ProcessAggregation() {
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
                                                           null,
                                                           null,
                                                           "com.bc.calvalus.processing.l3.SomethingElse",
                                                           "L3",
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
        assertThat(codeReport.toJson(PropertiesWrapper.get("mandatory.fields")), containsString("{\n" +
                                                       "  \"requestId\": \"job-01\",\n" +
                                                       "  \"jobName\": \"Job 01\",\n" +
                                                       "  \"jobSubmissionTime\": \"2018-01-01T01:00:00.000Z\",\n" +
                                                       "  \"userName\": \"user\",\n" +
                                                       "  \"queueName\": \"queue\",\n" +
                                                       "  \"inProducts\": \"/path/to/output\",\n" +
                                                       "  \"inProductsType\": \"/path/to/input\",\n" +
                                                       "  \"inCollection\": \"S2.L1C\",\n" +
                                                       "  \"inProductsNumber\": 5,\n" +
                                                       "  \"inProductsSize\": 122.281036,\n" +
                                                       "  \"requestSource\": \"BC Calvalus\",\n" +
                                                       "  \"processingCenter\": \"Calvalus\",\n" +
                                                       "  \"configuredCpuCoresPerTask\": 2,\n" +
                                                       "  \"cpuCoreHours\": 0.016489,\n" +
                                                       "  \"processorName\": \"Aggregation\",\n" +
                                                       "  \"configuredRamPerTask\": 4.000000,\n" +
                                                       "  \"ramHours\": 1.288120,\n" +
                                                       "  \"processingWorkflow\": \"L3\",\n" +
                                                       "  \"duration\": 82800.000000,\n" +
                                                       "  \"processingStatus\": \"SUCCESSFUL\",\n" +
                                                       "  \"outProductsNumber\": 5,\n" +
                                                       "  \"outProductsType\": \"S2_MASKED\",\n" +
                                                       "  \"outCollection\": \"Job 01\",\n" +
                                                       "  \"outProductsLocation\": \"file:/calvalus/home/user/12345\",\n" +
                                                       "  \"outProductsSize\": 0.767787,\n" +
                                                       "  \"messageType\": \"ProductProcessedMessage\",\n" +
                                                       "  \"serviceId\": \"code-de-processing-service\",\n"));
        assertThat(codeReport.toJson(PropertiesWrapper.get("mandatory.fields")), containsString("  \"version\": \"1.0\"\n" +
                                                       "}"));
    }

    @Test
    public void canRoundDecimalNumbersWhenDeserialize() {
        CodeReport codeReport = new CodeReport("id",
                                               "job",
                                               "jobSubmissionTime",
                                               "userName",
                                               "queueName",
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
                                               "processorDescription",
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
        assertThat(codeReport.toJson(PropertiesWrapper.get("mandatory.fields")), containsString("{\n" +
                                                       "  \"requestId\": \"id\",\n" +
                                                       "  \"jobName\": \"job\",\n" +
                                                       "  \"jobSubmissionTime\": \"jobSubmissionTime\",\n" +
                                                       "  \"userName\": \"userName\",\n" +
                                                       "  \"queueName\": \"queueName\",\n" +
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
                                              "  \"processorDescription\": \"processorDescription\",\n" +
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
        assertThat(codeReport.toJson(PropertiesWrapper.get("mandatory.fields")), containsString("  \"version\": \"1.0\"\n" +
                                                       "}"));
    }
}