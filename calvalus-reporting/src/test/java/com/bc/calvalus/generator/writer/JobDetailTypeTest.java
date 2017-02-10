package com.bc.calvalus.generator.writer;

import java.time.Instant;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * @author muhammad.bc.
 */
public class JobDetailTypeTest {
    @Test
    public void testTheSame() throws Exception {
        JobDetailType jobDetailType = new JobDetailType();
        jobDetailType.setJobId("123");

        JobDetailType secondDetailType = new JobDetailType();
        secondDetailType.setJobId("123");

        assertTrue(jobDetailType.equals(secondDetailType));
    }

    @Test
    public void testAfterDateTime() throws Exception {
        JobDetailType jobDetails1 = createJobDetails("123", "2017-01-01T00:30:00.00Z");

        String afterDateTime = getMilliSecondToString("2017-01-01T00:31:10.00Z");
        String beforeDateTime = getMilliSecondToString("2017-01-01T00:29:10.00Z");
        String sameDateTime = getMilliSecondToString("2017-01-01T00:30:00.00Z");

        assertTrue(jobDetails1.compareTo(afterDateTime) == -1);
        assertTrue(jobDetails1.compareTo(beforeDateTime) == 1);
        assertTrue(jobDetails1.compareTo(sameDateTime) == 0);
    }

    private String getMilliSecondToString(String dateTime) {
        long beforeDateTime = Instant.parse(dateTime).toEpochMilli();
        return Long.toString(beforeDateTime);
    }

    @NotNull
    private JobDetailType createJobDetails(String jobId, String date) {
        JobDetailType jobDetailType = new JobDetailType();
        jobDetailType.setJobId(jobId);
        String finishTime = Long.toString(Instant.parse(date).toEpochMilli());
        jobDetailType.setFinishTime(finishTime);
        return jobDetailType;
    }
}