package com.bc.calvalus.reporting.collector;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.bc.calvalus.reporting.collector.exception.ReportingCollectorException;
import org.junit.*;
import org.junit.rules.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author hans
 */
public class JobReportsTest {

    private static final String UNKNOWN_REPORT_NAME = "unknown";
    private String sampleJobsReportPathString;

    private JobReports jobReports;

    @Rule
    public ExpectedException thrownException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        jobReports = new JobReports();
        this.sampleJobsReportPathString = getResourcePathString("sample-jobs-report.json");
    }

    @Test
    public void canInitialize() throws Exception {
        jobReports.init(this.sampleJobsReportPathString);

        assertThat(jobReports.contains("job_1484837520075_13160"), equalTo(true));
        assertThat(jobReports.contains("unknown_job_id"), equalTo(false));
    }

    @Test
    public void canCreateNewReportWhenNoJobsReportFound() throws Exception {
        jobReports.init(UNKNOWN_REPORT_NAME);

        assertThat(Files.exists(Paths.get(UNKNOWN_REPORT_NAME)), equalTo(true));
        assertThat(jobReports.getKnownJobIdSet().size(), equalTo(0));

        // to cleanup
        Files.delete(Paths.get(UNKNOWN_REPORT_NAME));
        assertThat(Files.exists(Paths.get(UNKNOWN_REPORT_NAME)), equalTo(false));
    }

    @Test
    public void canCatchIOException() throws Exception {
        thrownException.expect(ReportingCollectorException.class);
        thrownException.expectMessage("Unable to open job reports file 'imaginary/unknown'");

        jobReports.init("imaginary/unknown");
    }

    private String getResourcePathString(String resourceName) throws URISyntaxException, IOException {
        URL jobsReportUrl = this.getClass().getClassLoader().getResource(resourceName);
        if (jobsReportUrl != null) {
            return Paths.get(jobsReportUrl.toURI()).toString();
        } else {
            File tempFile = File.createTempFile(resourceName, ".json");
            return tempFile.getAbsolutePath();
        }
    }
}