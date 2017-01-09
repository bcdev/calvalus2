package com.bc.calvalus.generator.writer;

import com.bc.calvalus.generator.FileConnectionCheckIO;
import com.bc.calvalus.generator.GenerateLogException;
import com.bc.calvalus.generator.TestUtils;
import com.bc.calvalus.generator.extractor.JobExtractor;
import com.bc.calvalus.generator.extractor.jobs.JobType;
import com.bc.calvalus.generator.extractor.jobs.JobsType;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

/**
 * @author muhammad.bc.
 */
@RunWith(FileConnectionCheckIO.class)
public class WriteJobDetailTest {
    private WriteJobDetail writeJobDetail;

    @Before
    public void setUp() throws Exception {
        writeJobDetail = new WriteJobDetail(TestUtils.getSaveLocation());
    }

    @Test
    public void testWriteToFileJobID() throws Exception {
        JobsType jobsType = new JobExtractor().getJobsType();
        List<JobType> jobTypeList = jobsType.getJob();
        int randomNum = ThreadLocalRandom.current().nextInt(0, jobTypeList.size() + 1);
        JobType jobType = jobTypeList.get(randomNum);
        String jobTypeId = jobType.getId();
        writeJobDetail.write(jobTypeId);
    }

    @Test
    public void testWithRange() throws Exception {
        writeJobDetail.write(3);
    }

    @Test
    public void testWithInterval() throws Exception {
        writeJobDetail.write(0, 3);

    }

    @Test(expected = GenerateLogException.class)
    public void testOutInterval() throws Exception {
        writeJobDetail.write(-3, 3);
    }

    @Test
    public void testTheSameInterval() throws Exception {
        writeJobDetail.write(3, 3);
    }

    @Test
    @Ignore
    public void testOutputNotExist() throws Exception {
        try {
            new WriteJobDetail("cool");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("The folder does not exist."));
        }
    }

    @Test
    public void testGetLastJobID() throws Exception {

        WriteJobDetail writeJobDetail = new WriteJobDetail(TestUtils.getSaveLocation());
        String lastJobID = writeJobDetail.getLastJobID();
        assertNotNull(lastJobID);

        int[] rangeIndex = writeJobDetail.getStartStopIndex(lastJobID);
        writeJobDetail.write(rangeIndex[0], rangeIndex[0] + 3);
    }
}