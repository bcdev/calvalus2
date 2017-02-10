package com.bc.calvalus.generator.writer;

import java.io.File;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Ignore;
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
        Assert.assertEquals(getEOFJobInfo.getLastJobDetailsType().getJobId(), "job_1484434434570_0374");
    }

    @Ignore
    @Test
    public void testwriteJobEOF() throws Exception {
        File outputFile = new File(pathname);
        JobDetailWriter detailWriter = new JobDetailWriter(outputFile.getPath());
        JobDetailType jobDetailType = new JobDetailType();
        jobDetailType.setUser("you");
        jobDetailType.setJobId("102");
        jobDetailType.setCpuMilliseconds("10");
        jobDetailType.setTotalMaps("200");
        detailWriter.flushToFile(Collections.singletonList(jobDetailType), outputFile);


        JobDetailWriter.GetEOFJobInfo getEOFJobInfo = new JobDetailWriter.GetEOFJobInfo(outputFile);
        Assert.assertEquals(getEOFJobInfo.getLastJobDetailsType(), "102");
    }
}