package com.bc.calvalus.reporting.collector;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.*;

import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;

/**
 * @author hans
 */
public class JobReportsTest {

    private URI sampleJobsReportUri;

    private JobReports jobReports;

    @Before
    public void setUp() throws Exception {
        jobReports = new JobReports();
        URL sampleJobsReportUrl = this.getClass().getClassLoader().getResource("sample-jobs-report.json");
        this.sampleJobsReportUri = new URI("");
        if (sampleJobsReportUrl != null) {
            this.sampleJobsReportUri = sampleJobsReportUrl.toURI();
        }
    }

    @Test
    public void canInitialize() throws Exception {
        jobReports.init(Paths.get(this.sampleJobsReportUri).toString());

        assertThat(jobReports.contains("job_1484837520075_13160"), equalTo(true));
        assertThat(jobReports.contains("unknown_job_id"), equalTo(false));
    }

    @Test
    public void canThrowExceptionWhenNoJobsReportFound() throws Exception {

    }
}