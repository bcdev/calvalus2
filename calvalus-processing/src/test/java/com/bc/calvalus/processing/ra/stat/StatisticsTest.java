package com.bc.calvalus.processing.ra.stat;

import org.junit.Test;

import java.io.StringWriter;

import static org.junit.Assert.*;

public class StatisticsTest {

    @Test
    public void test_empty() throws Exception {
        StringWriter stringWriter = new StringWriter();
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10");
        Statistics statistics = new Statistics(dateRanges, new String[]{"b1", "b2"}, stringWriter);
        stringWriter.close();
        assertEquals("RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\tb2_count\tb2_min\tb2_max\tb2_arithMean\tb2_sigma\tb2_geomMean\tb2_p5\tb2_p25\tb2_p50\tb2_p75\tb2_p95\n", stringWriter.toString());
    }

    @Test
    public void test() throws Exception {
        StringWriter stringWriter = new StringWriter();
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10");
        Statistics statistics = new Statistics(dateRanges, new String[]{"b1", "b2"}, stringWriter);
        stringWriter.close();
        String s = stringWriter.toString();
        System.out.println(s);
    }

}