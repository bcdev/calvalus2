package com.bc.calvalus.extractor.writer;

import java.io.File;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author muhammad.bc.
 */
public class JobDetailWriterTest {
    private String pathname = getClass().getClassLoader().getResource(".").getFile() + File.separator + "calvalus-reporting.json";
    private JobDetailWriter jobDetailWriter;

    @Before
    public void setUp() throws Exception {
        jobDetailWriter = new JobDetailWriter();
    }

    @Test
    public void readLastJobID() throws Exception {
        File file = new File(pathname);
        JobDetailWriter.GetEOFJobInfo getEOFJobInfo = new JobDetailWriter.GetEOFJobInfo(file);
        assertEquals(getEOFJobInfo.getLastJobDetailsType().getJobId(), "job_1484434434570_0376");
    }

    @Test
    public void testSingleDateTimePredicate() throws Exception {
        Predicate<String> stringPredicate = jobDetailWriter.filterDateTime("2016-12-20T00:00");
        assertTrue(stringPredicate.test("2016-12-01_To_2016-12-31.json"));
        assertTrue(stringPredicate.test("2016-12-10_To_2016-12-31.json"));
        assertFalse(stringPredicate.test("2017-12-10_To_2016-12-31.json"));
    }

    @Test
    public void testBetweenDateTimePredicate() throws Exception {
        List<String> fileName = Arrays.asList(
                "2014-12-01_To_2014-12-31.json",
                "2015-03-01_To_2015-01-31.json",
                "2016-11-01_To_2016-11-30.json",
                "2016-03-01_To_2016-03-31.json",
                "2017-09-01_To_2017-12-31.json",
                "2016-12-01_To_2016-12-31.json",
                "2018-06-01_To_2018-06-30.json",
                "2019-07-01_To_2019-07-31.json",
                "2022-12-01_To_2022-12-31.json");


        Predicate<String> stringPredicate = jobDetailWriter.filterDateTimeBtw("2016-12-20T00:00", "2016-12-12T00:00");
        List<String> collectFileBtwn = fileName.stream().filter(stringPredicate).collect(Collectors.toList());
        assertThat(collectFileBtwn.size(), is(1));



        Predicate<String> stringPredicateRange = jobDetailWriter.filterDateTimeBtw("2016-12-20T00:00", "2022-12-12T00:00");
        List<String> collectBtwn = fileName.stream().filter(stringPredicateRange).collect(Collectors.toList());
        assertThat(collectBtwn.size(), is(5));

    }

    @Test
    public void firstDayOfMonth() throws Exception {
        assertThat(jobDetailWriter.getFirstDayOfMonth("2017-04-12T02:00").toString(), is("2017-04-01T00:00"));
        assertThat(jobDetailWriter.getFirstDayOfMonth("2020-02-29T23:00").toString(), is("2020-02-01T00:00"));
        assertThat(jobDetailWriter.getFirstDayOfMonth("2020-02-11T23:00").toString(), is("2020-02-01T00:00"));
    }

    @Test
    public void lastDayOfMonth() throws Exception {
        assertThat(jobDetailWriter.getLastDayOfMonth("2014-03-12T02:00").toString(), is("2014-03-31T00:00"));
        assertThat(jobDetailWriter.getLastDayOfMonth("2019-03-12T02:00").toString(), is("2019-03-31T00:00"));
        assertThat(jobDetailWriter.getLastDayOfMonth("2020-12-12T02:00").toString(), is("2020-12-31T00:00"));
        assertThat(jobDetailWriter.getLastDayOfMonth("2021-10-12T02:00").toString(), is("2021-10-31T00:00"));
    }

    @Test
    public void testDateNonExist() throws Exception {
        try {
            assertThat(jobDetailWriter.getFirstDayOfMonth("2020/02/29T23:00").toString(), is("2020-02-01T00:00"));
            fail();
        } catch (DateTimeParseException e) {

        }
    }

    private JobDetailType createJobDetailType(String finishTime) {
        JobDetailType jobDetailType = new JobDetailType();
        jobDetailType.setFinishTime(finishTime);
        return jobDetailType;
    }
}