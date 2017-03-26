package com.bc.calvalus.processing.ra.stat;

import org.junit.Test;

import java.io.StringWriter;
import java.text.DateFormat;

import static org.junit.Assert.*;

public class StatisticsTest {

    private static DateFormat dateFormat = RADateRanges.createDateFormat();

    @Test
    public void test_empty() throws Exception {
        StringWriter stringWriter = new StringWriter();
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10");
        new Statistics(dateRanges, new String[]{"b1", "b2"}, stringWriter);
        stringWriter.close();
        assertEquals("RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\tb2_count\tb2_min\tb2_max\tb2_arithMean\tb2_sigma\tb2_geomMean\tb2_p5\tb2_p25\tb2_p50\tb2_p75\tb2_p95\n", stringWriter.toString());
    }

    @Test
    public void test_oneRange_nodata() throws Exception {
        StringWriter stringWriter = new StringWriter();
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10");
        Statistics statistics = new Statistics(dateRanges, new String[]{"b1"}, stringWriter);
        statistics.startRegion("r1");
        statistics.endRegion();
        stringWriter.close();
        assertEquals("RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n", stringWriter.toString());
    }

    @Test
    public void test_oneRange_data() throws Exception {
        StringWriter stringWriter = new StringWriter();
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10");
        Statistics statistics = new Statistics(dateRanges, new String[]{"b1"}, stringWriter);
        statistics.startRegion("r1");
        statistics.addData(dateFormat.parse("2010-01-01 10:00:00").getTime(), 7, new float[][]{{1, 2, 3}});
        statistics.endRegion();
        stringWriter.close();
        assertEquals("RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t1\t7\t3\t1.0\t3.0\t2.0\t0.8164965809277263\t1.8171205928321397\t1.0\t1.0\t2.0\t3.0\t3.0\n", stringWriter.toString());
    }

    @Test
    public void test_manyRanges_nodata() throws Exception {
        StringWriter stringWriter = new StringWriter();
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10,2010-01-11:2010-01-20,2010-01-21:2010-01-31,2010-02-01:2010-02-11");
        Statistics statistics = new Statistics(dateRanges, new String[]{"b1"}, stringWriter);
        statistics.startRegion("r1");
        statistics.endRegion();
        stringWriter.close();
        assertEquals("RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r1\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r1\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r1\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n", stringWriter.toString());
    }

    @Test
    public void test_manyRanges_data() throws Exception {
        StringWriter stringWriter = new StringWriter();
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10,2010-01-11:2010-01-20,2010-01-21:2010-01-31,2010-02-01:2010-02-11");
        Statistics statistics = new Statistics(dateRanges, new String[]{"b1"}, stringWriter);
        statistics.startRegion("r1");
        statistics.addData(dateFormat.parse("2010-01-15 10:00:00").getTime(), 7, new float[][]{{1, 2, 3}});
        statistics.addData(dateFormat.parse("2010-01-15 11:00:00").getTime(), 8, new float[][]{{4, 5, 6}});
        statistics.addData(dateFormat.parse("2010-01-15 12:00:00").getTime(), 9, new float[][]{{7, 8, 9}});
        statistics.addData(dateFormat.parse("2010-02-03 12:00:00").getTime(), 11, new float[][]{{3, 4, 5}});
        statistics.endRegion();
        stringWriter.close();
        assertEquals("RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r1\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t3\t24\t9\t1.0\t9.0\t5.0\t2.5819888974716116\t4.147166274396913\t1.0\t2.5\t5.0\t7.5\t9.0\n" +
                "r1\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r1\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t1\t11\t12\t1.0\t9.0\t4.75\t2.3139072294858036\t4.087830285940099\t1.0\t3.0\t4.5\t6.75\t9.0\n", stringWriter.toString());
    }

    @Test
    public void test_manyRanges_data_multipleRegions() throws Exception {
        StringWriter stringWriter = new StringWriter();
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10,2010-01-11:2010-01-20,2010-01-21:2010-01-31,2010-02-01:2010-02-11");
        Statistics statistics = new Statistics(dateRanges, new String[]{"b1"}, stringWriter);
        statistics.startRegion("r1");
        statistics.addData(dateFormat.parse("2010-01-15 10:00:00").getTime(), 7, new float[][]{{1, 2, 3}});
        statistics.addData(dateFormat.parse("2010-01-15 11:00:00").getTime(), 8, new float[][]{{4, 5, 6}});
        statistics.addData(dateFormat.parse("2010-01-15 12:00:00").getTime(), 9, new float[][]{{7, 8, 9}});
        statistics.addData(dateFormat.parse("2010-02-03 12:00:00").getTime(), 11, new float[][]{{3, 4, 5}});
        statistics.endRegion();
        statistics.startRegion("r2");
        statistics.endRegion();
        statistics.startRegion("r3");
        statistics.endRegion();
        stringWriter.close();
        assertEquals("RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r1\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t3\t24\t9\t1.0\t9.0\t5.0\t2.5819888974716116\t4.147166274396913\t1.0\t2.5\t5.0\t7.5\t9.0\n" +
                "r1\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r1\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t1\t11\t12\t1.0\t9.0\t4.75\t2.3139072294858036\t4.087830285940099\t1.0\t3.0\t4.5\t6.75\t9.0\n" +
                "r2\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r2\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r2\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r2\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r3\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r3\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r3\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r3\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n", stringWriter.toString());
    }

}