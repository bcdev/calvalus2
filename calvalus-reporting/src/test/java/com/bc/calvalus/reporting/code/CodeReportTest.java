package com.bc.calvalus.reporting.code;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.bc.calvalus.reporting.urban.reporting.CalvalusReport;
import org.junit.*;

/**
 * @author hans
 */
public class CodeReportTest {

    @Test
    public void canParseCorrectly() {
        CalvalusReport calvalusReport = new CalvalusReport("job-01",
                                                           "user",
                                                           "queue",
                                                           1514764800000L,
                                                           1514768400000L,
                                                           1514851200000L,
                                                           "SUCCESSFUL",
                                                           "/path/to/output",
                                                           "Job 01",
                                                           "remoteUser",
                                                           "remoteRef",
                                                           "Fmask",
                                                           "L2",
                                                           "/path/to/input",
                                                           "S2.L1C",
                                                           "2",
                                                           "4096",
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
        CodeReport codeReport = new CodeReport(calvalusReport);
        assertThat(codeReport.toJson(), containsString("{\n" +
                                                       "  \"requestId\": \"job-01\",\n" +
                                                       "  \"jobName\": \"Job 01\",\n" +
                                                       "  \"jobSubmissionTime\": \"2018-01-01T01:00:00.000Z\",\n" +
                                                       "  \"userName\": \"user\",\n" +
                                                       "  \"inCollection\": \"S2.L1C\",\n" +
                                                       "  \"inProductsNumber\": 5,\n" +
                                                       "  \"inProductsSize\": 122.281036,\n" +
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
                                                       "  \"outCollection\": \"Job 01\",\n" +
                                                       "  \"outProductsLocation\": \"/path/to/output\",\n" +
                                                       "  \"outProductsSize\": 0.767787,\n" +
                                                       "  \"messageType\": \"ProductProcessedMessage\",\n" +
                                                       "  \"serviceId\": \"code-de-processing-service\""));
        assertThat(codeReport.toJson(), containsString("  \"version\": \"1.0\"\n" +
                                                       "}"));
    }

    @Test
    public void canDeserialize() {
        CodeReport codeReport = new CodeReport("id",
                                               "job",
                                               "jobSubmissionTime",
                                               "userName",
                                               "inCollection",
                                               0,
                                               0.123456789,
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
                                               "outCollection",
                                               "outProductsLocation",
                                               0.123456789
        );
        assertThat(codeReport.toJson(), containsString("{\n" +
                                                       "  \"requestId\": \"id\",\n" +
                                                       "  \"jobName\": \"job\",\n" +
                                                       "  \"jobSubmissionTime\": \"jobSubmissionTime\",\n" +
                                                       "  \"userName\": \"userName\",\n" +
                                                       "  \"inCollection\": \"inCollection\",\n" +
                                                       "  \"inProductsNumber\": 0,\n" +
                                                       "  \"inProductsSize\": 0.123457,\n" +
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
                                                       "  \"outCollection\": \"outCollection\",\n" +
                                                       "  \"outProductsLocation\": \"outProductsLocation\",\n" +
                                                       "  \"outProductsSize\": 0.123457,\n" +
                                                       "  \"messageType\": \"ProductProcessedMessage\",\n" +
                                                       "  \"serviceId\": \"code-de-processing-service\",\n"));
        assertThat(codeReport.toJson(), containsString("  \"version\": \"1.0\"\n" +
                                                       "}"));
    }
}