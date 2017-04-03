package com.bc.calvalus.extractor.writer;

import com.bc.wps.utilities.PropertiesWrapper;
import java.io.File;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author muhammad.bc.
 */
public class JobDetailWriterTest {
    private String pathname = getClass().getClassLoader().getResource(".").getFile() + File.separator + "calvalus-reporting.json";
    private JobDetailWriter jobDetailWriter;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("conf/calvalus-reporting.properties");
        jobDetailWriter = new JobDetailWriter();
    }

    @Test
    public void readLastJobID() throws Exception {
        File file = new File(pathname);
        JobDetailWriter.GetEOFJobInfo getEOFJobInfo = new JobDetailWriter.GetEOFJobInfo(file);
        assertEquals(getEOFJobInfo.getLastJobDetailsType().getJobId(), "job_1484434434570_0376");
    }


    private JobDetailType createJobDetailType(String finishTime) {
        JobDetailType jobDetailType = new JobDetailType();
        jobDetailType.setFinishTime(finishTime);
        return jobDetailType;
    }
}