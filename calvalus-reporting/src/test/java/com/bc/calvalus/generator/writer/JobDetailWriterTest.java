package com.bc.calvalus.generator.writer;

import java.io.File;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author muhammad.bc.
 */
public class JobDetailWriterTest {
    private String pathname = getClass().getClassLoader().getResource(".").getFile() + File.separator + "calvalus-reporting.json";

    @Test
    public void readLastJobID() throws Exception {
        File file = new File(pathname);
        JobDetailWriter.GetEOFJobInfo getEOFJobInfo = new JobDetailWriter.GetEOFJobInfo(file);
        Assert.assertEquals(getEOFJobInfo.getLastJobDetailsType().getJobId(), "job_1484434434570_0376");
    }
}