package com.bc.calvalus.reporting.collector;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.bc.calvalus.reporting.collector.exception.JobReportsException;
import com.bc.wps.utilities.PropertiesWrapper;
import org.apache.commons.io.FileUtils;
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

    private static final String NEW_REPORT_DIR = "temp-dir";
    private static final long DUMMY_FINISH_TIME = 1499566932435L;
    private static final long DUMMY_FINISH_TIME2 = 1486060721723L;
    private String sampleJobsReportPathString;

    private JobReports jobReports;

    @Rule
    public ExpectedException thrownException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("reporting-collector-test.properties");
        jobReports = new JobReports();
        this.sampleJobsReportPathString = getResourcePathString(PropertiesWrapper.get("reporting.folder.path"));
    }

    @Test
    public void canInitialize() throws Exception {
        jobReports.init(this.sampleJobsReportPathString);

        assertThat(jobReports.getKnownJobIdSet().size(), equalTo(13));
        assertThat(jobReports.contains("job_1484837520075_13150"), equalTo(true));
        assertThat(jobReports.contains("unknown_job_id"), equalTo(false));
    }

    @Test
    public void canCreateNewReportWhenNoJobsReportFound() throws Exception {
        jobReports.init(NEW_REPORT_DIR);

        assertThat(Files.exists(Paths.get(NEW_REPORT_DIR)), equalTo(true));
        assertThat(jobReports.getKnownJobIdSet().size(), equalTo(0));

        jobReports.closeBufferedWriters();

        cleanUp();
    }

    @Test
    public void canAppend() throws Exception {
        jobReports.init(NEW_REPORT_DIR);

        assertThat(Files.exists(Paths.get(NEW_REPORT_DIR)), equalTo(true));
        assertThat(jobReports.getKnownJobIdSet().size(), equalTo(0));

        jobReports.add("1", DUMMY_FINISH_TIME, getSingleJsonReportEntry());
        assertThat(jobReports.contains("1"), equalTo(true));
        assertThat(jobReports.getKnownJobIdSet().size(), equalTo(1));

        jobReports.add("2", DUMMY_FINISH_TIME2, getSingleJsonReportEntry());
        assertThat(jobReports.contains("2"), equalTo(true));
        assertThat(jobReports.getKnownJobIdSet().size(), equalTo(2));
        assertThat(jobReports.contains("1"), equalTo(true));
        assertThat(jobReports.contains("2"), equalTo(true));

        jobReports.closeBufferedWriters();

        cleanUp();
    }

    @Test
    public void canCatchIOException() throws Exception {
        thrownException.expect(JobReportsException.class);
        thrownException.expectMessage("Unable to create directory 'imaginary/unknown'");

        jobReports.init("imaginary/unknown");
    }

    private void cleanUp() throws IOException {
        FileUtils.deleteDirectory(Paths.get(NEW_REPORT_DIR).toFile());
        assertThat(Files.exists(Paths.get(NEW_REPORT_DIR)), equalTo(false));
    }

    private String getSingleJsonReportEntry() {
        return "{\"jobId\":\"1\",\"user\":\"test\",\"queue\":\"testing\",\"startTime\":\"1\",\"finishTime\":\"2\",\"mapsCompleted\":\"1\",\"reducesCompleted\":\"0\",\"state\":\"SUCCEEDED\",\"inputPath\":\"dummyInputPath\",\"fileBytesRead\":\"0\",\"fileBytesWritten\":\"64\",\"hdfsBytesRead\":\"128\",\"hdfsBytesWritten\":\"64\",\"vCoresMillisTotal\":\"200\",\"mbMillisMapTotal\":\"1000\",\"cpuMilliseconds\":\"1000\",\"totalMaps\":\"1\",\"jobName\":\"dummyName\"},";
    }

    private String getResourcePathString(String resourceName) throws URISyntaxException, IOException {
        URL jobsReportUrl = this.getClass().getClassLoader().getResource(resourceName);
        if (jobsReportUrl != null) {
            return Paths.get(jobsReportUrl.toURI()).toString();
        } else {
            File tempFile = File.createTempFile(resourceName, null);
            return tempFile.getAbsolutePath();
        }
    }
}