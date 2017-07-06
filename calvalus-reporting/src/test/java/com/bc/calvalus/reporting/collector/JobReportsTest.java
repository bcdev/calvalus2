package com.bc.calvalus.reporting.collector;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.bc.calvalus.reporting.collector.exception.JobReportsException;
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

    private static final String NEW_REPORT_NAME = "new-jobs-report.json";
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

        assertThat(jobReports.getKnownJobIdSet().size(), equalTo(14));
        assertThat(jobReports.contains("job_1484837520075_13160"), equalTo(true));
        assertThat(jobReports.contains("unknown_job_id"), equalTo(false));
    }

    @Test
    public void canCreateNewReportWhenNoJobsReportFound() throws Exception {
        jobReports.init(NEW_REPORT_NAME);

        assertThat(Files.exists(Paths.get(NEW_REPORT_NAME)), equalTo(true));
        assertThat(jobReports.getKnownJobIdSet().size(), equalTo(0));

        jobReports.closeBufferedWriter();

        cleanUp();
    }

    @Test
    public void canAppend() throws Exception {
        jobReports.init(NEW_REPORT_NAME);

        assertThat(Files.exists(Paths.get(NEW_REPORT_NAME)), equalTo(true));
        assertThat(jobReports.getKnownJobIdSet().size(), equalTo(0));

        jobReports.add("1", getSingleJsonReportEntry());
        assertThat(jobReports.contains("1"), equalTo(true));
        assertThat(jobReports.getKnownJobIdSet().size(), equalTo(1));

        jobReports.add("2", getSingleJsonReportEntry());
        assertThat(jobReports.contains("2"), equalTo(true));
        assertThat(jobReports.getKnownJobIdSet().size(), equalTo(2));
        assertThat(jobReports.contains("1"), equalTo(true));
        assertThat(jobReports.contains("2"), equalTo(true));

        jobReports.flushBufferedWriter();
        jobReports.closeBufferedWriter();

        cleanUp();
    }

    @Test
    public void canCatchIOException() throws Exception {
        thrownException.expect(JobReportsException.class);
        thrownException.expectMessage("Unable to open job reports file 'imaginary/unknown'");

        jobReports.init("imaginary/unknown");
    }

    private void cleanUp() throws IOException {
        Files.delete(Paths.get(NEW_REPORT_NAME));
        assertThat(Files.exists(Paths.get(NEW_REPORT_NAME)), equalTo(false));
    }

    private String getSingleJsonReportEntry() {
        return "{\"jobId\":\"1\",\"user\":\"test\",\"queue\":\"testing\",\"startTime\":\"1\",\"finishTime\":\"2\",\"mapsCompleted\":\"1\",\"reducesCompleted\":\"0\",\"state\":\"SUCCEEDED\",\"inputPath\":\"dummyInputPath\",\"fileBytesRead\":\"0\",\"fileBytesWritten\":\"64\",\"hdfsBytesRead\":\"128\",\"hdfsBytesWritten\":\"64\",\"vCoresMillisTotal\":\"200\",\"mbMillisMapTotal\":\"1000\",\"cpuMilliseconds\":\"1000\",\"totalMaps\":\"1\",\"jobName\":\"dummyName\"},";
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