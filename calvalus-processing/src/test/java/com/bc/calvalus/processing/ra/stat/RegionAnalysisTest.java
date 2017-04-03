package com.bc.calvalus.processing.ra.stat;

import com.bc.calvalus.processing.ra.RAConfig;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RegionAnalysisTest {

    private static DateFormat dateFormat = RADateRanges.dateFormat;

    private RAConfig oneBand;
    private RAConfig twoBands;
    private RAConfig.BandConfig b1;
    private RAConfig.BandConfig b2;
    private InMemWriterFactory writer;

    @Before
    public void setUp() throws Exception {
        b1 = new RAConfig.BandConfig("b1", 5, 0.0, 10.0);
        b2 = new RAConfig.BandConfig("b2", 5, 0.0, 10.0);
        oneBand = new RAConfig();
        oneBand.setBandConfigs(b1);
        oneBand.setInternalRegionNames("r1");
        oneBand.setWriteStatisticsFilePerRegion(false);
        oneBand.setWriteSeparateHistogramFile(true);
        twoBands = new RAConfig();
        twoBands.setBandConfigs(b1, b2);
        twoBands.setInternalRegionNames("r1");
        twoBands.setWriteStatisticsFilePerRegion(false);
        twoBands.setWriteSeparateHistogramFile(true);
        writer = new InMemWriterFactory();
    }

    @Test
    public void test_empty() throws Exception {
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10");
        RegionAnalysis ra = new RegionAnalysis(dateRanges, oneBand, writer);
        ra.close();

        Set<String> keySet = writer.writerMap.keySet();
        assertEquals(2, keySet.size());
        assertTrue(keySet.contains("region-statistics.csv"));
        assertTrue(keySet.contains("region-histogram-b1.csv"));

        String expected = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n";
        String actual = writer.writerMap.get("region-statistics.csv").toString();
        assertEquals(expected, actual);

        expected = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_belowHistogram\tb1_aboveHistogram\tb1_numBins\tb1_lowValue\tb1_highValue\tb1_bin_0\tb1_bin_1\tb1_bin_2\tb1_bin_3\tb1_bin_4\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n";
        actual = writer.writerMap.get("region-histogram-b1.csv").toString();
        assertEquals(expected, actual);
    }

    @Test
    public void test_oneRange_nodata() throws Exception {
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10");
        RegionAnalysis ra = new RegionAnalysis(dateRanges, oneBand, writer);
        ra.startRegion("r1");
        ra.endRegion();
        ra.close();

        Set<String> keySet = writer.writerMap.keySet();
        assertEquals(2, keySet.size());
        assertTrue(keySet.contains("region-statistics.csv"));
        assertTrue(keySet.contains("region-histogram-b1.csv"));

        String actual = writer.writerMap.get("region-statistics.csv").toString();
        assertEquals("RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\n" +
                             "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n", actual);
    }

    @Test
    public void test_oneRange_data() throws Exception {
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10");
        RegionAnalysis ra = new RegionAnalysis(dateRanges, oneBand, writer);
        ra.startRegion("r1");
        ra.addData(dateFormat.parse("2010-01-01 10:00:00").getTime(), 7, new float[][]{{1, 2, 3}});
        ra.endRegion();
        ra.close();

        Set<String> keySet = writer.writerMap.keySet();
        assertEquals(2, keySet.size());
        assertTrue(keySet.contains("region-statistics.csv"));
        assertTrue(keySet.contains("region-histogram-b1.csv"));

        String expected = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t1\t7\t3\t1.0\t3.0\t2.0\t0.8164965809277263\t1.8171205928321397\t1.0\t1.0\t2.0\t3.0\t3.0\n";
        String actual = writer.writerMap.get("region-statistics.csv").toString();
        assertEquals(expected, actual);

        expected = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_belowHistogram\tb1_aboveHistogram\tb1_numBins\tb1_lowValue\tb1_highValue\tb1_bin_0\tb1_bin_1\tb1_bin_2\tb1_bin_3\tb1_bin_4\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t1\t7\t0\t0\t5\t0.0\t10.0\t1\t2\t0\t0\t0\n";
        actual = writer.writerMap.get("region-histogram-b1.csv").toString();
        assertEquals(expected, actual);
    }

    @Test
    public void test_manyRanges_nodata() throws Exception {
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10,2010-01-11:2010-01-20,2010-01-21:2010-01-31,2010-02-01:2010-02-11");
        RegionAnalysis ra = new RegionAnalysis(dateRanges, oneBand, writer);
        ra.startRegion("r1");
        ra.endRegion();
        ra.close();

        Set<String> keySet = writer.writerMap.keySet();
        assertEquals(2, keySet.size());
        assertTrue(keySet.contains("region-statistics.csv"));
        assertTrue(keySet.contains("region-histogram-b1.csv"));

        String expected = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r1\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r1\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r1\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n";
        String actual = writer.writerMap.get("region-statistics.csv").toString();
        assertEquals(expected, actual);

        expected = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_belowHistogram\tb1_aboveHistogram\tb1_numBins\tb1_lowValue\tb1_highValue\tb1_bin_0\tb1_bin_1\tb1_bin_2\tb1_bin_3\tb1_bin_4\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r1\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r1\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r1\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n";
        actual = writer.writerMap.get("region-histogram-b1.csv").toString();
        assertEquals(expected, actual);
    }

    @Test
    public void test_manyRanges_data() throws Exception {
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10,2010-01-11:2010-01-20,2010-01-21:2010-01-31,2010-02-01:2010-02-11");
        RegionAnalysis ra = new RegionAnalysis(dateRanges, oneBand, writer);
        ra.startRegion("r1");
        ra.addData(dateFormat.parse("2010-01-15 10:00:00").getTime(), 7, new float[][]{{1, 2, 3}});
        ra.addData(dateFormat.parse("2010-01-15 11:00:00").getTime(), 8, new float[][]{{4, 5, 6}});
        ra.addData(dateFormat.parse("2010-01-15 12:00:00").getTime(), 9, new float[][]{{7, 8, 9}});
        ra.addData(dateFormat.parse("2010-02-03 12:00:00").getTime(), 11, new float[][]{{3, 4, 5}});
        ra.endRegion();
        ra.close();

        Set<String> keySet = writer.writerMap.keySet();
        assertEquals(2, keySet.size());
        assertTrue(keySet.contains("region-statistics.csv"));
        assertTrue(keySet.contains("region-histogram-b1.csv"));

        String actual = writer.writerMap.get("region-statistics.csv").toString();
        String expected = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r1\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t3\t24\t9\t1.0\t9.0\t5.0\t2.5819888974716116\t4.147166274396913\t1.0\t2.5\t5.0\t7.5\t9.0\n" +
                "r1\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r1\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t1\t11\t3\t3.0\t5.0\t4.0\t0.8164965809277268\t3.9148676411688634\t3.0\t3.0\t4.0\t5.0\t5.0\n";
        assertEquals(expected, actual);

        expected = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_belowHistogram\tb1_aboveHistogram\tb1_numBins\tb1_lowValue\tb1_highValue\tb1_bin_0\tb1_bin_1\tb1_bin_2\tb1_bin_3\tb1_bin_4\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r1\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t3\t24\t0\t0\t5\t0.0\t10.0\t1\t2\t2\t2\t2\n" +
                "r1\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r1\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t1\t11\t0\t0\t5\t0.0\t10.0\t0\t1\t2\t0\t0\n";
        actual = writer.writerMap.get("region-histogram-b1.csv").toString();
        assertEquals(expected, actual);
    }

    @Test
    public void test_many_many() throws Exception {
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10,2010-01-11:2010-01-20,2010-01-21:2010-01-31,2010-02-01:2010-02-11");
        oneBand.setInternalRegionNames("r1", "r2", "r3");
        RegionAnalysis ra = new RegionAnalysis(dateRanges, oneBand, writer);
        ra.startRegion("r1");
        ra.addData(dateFormat.parse("2010-01-15 10:00:00").getTime(), 7, new float[][]{{1, 2, 3}});
        ra.addData(dateFormat.parse("2010-01-15 11:00:00").getTime(), 8, new float[][]{{4, 5, 6}});
        ra.addData(dateFormat.parse("2010-01-15 12:00:00").getTime(), 9, new float[][]{{7, 8, 9}});
        ra.addData(dateFormat.parse("2010-02-03 12:00:00").getTime(), 11, new float[][]{{3, 4, 5}});
        ra.endRegion();
        ra.startRegion("r2");
        ra.endRegion();
        ra.startRegion("r3");
        ra.endRegion();
        ra.close();

        Set<String> keySet = writer.writerMap.keySet();
        assertEquals(2, keySet.size());
        assertTrue(keySet.contains("region-statistics.csv"));
        assertTrue(keySet.contains("region-histogram-b1.csv"));

        String expected1 = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r1\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t3\t24\t9\t1.0\t9.0\t5.0\t2.5819888974716116\t4.147166274396913\t1.0\t2.5\t5.0\t7.5\t9.0\n" +
                "r1\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r1\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t1\t11\t3\t3.0\t5.0\t4.0\t0.8164965809277268\t3.9148676411688634\t3.0\t3.0\t4.0\t5.0\t5.0\n" +
                "r2\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r2\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r2\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r2\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r3\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r3\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r3\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r3\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n";
        String actual1 = writer.writerMap.get("region-statistics.csv").toString();
        assertEquals(expected1, actual1);

        String expected2 = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_belowHistogram\tb1_aboveHistogram\tb1_numBins\tb1_lowValue\tb1_highValue\tb1_bin_0\tb1_bin_1\tb1_bin_2\tb1_bin_3\tb1_bin_4\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r1\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t3\t24\t0\t0\t5\t0.0\t10.0\t1\t2\t2\t2\t2\n" +
                "r1\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r1\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t1\t11\t0\t0\t5\t0.0\t10.0\t0\t1\t2\t0\t0\n" +
                "r2\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r2\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r2\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r2\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r3\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r3\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r3\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r3\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n";
        String actual2 = writer.writerMap.get("region-histogram-b1.csv").toString();
        assertEquals(expected2, actual2);
    }

    @Test
    public void test_many_many_histoInside() throws Exception {
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10,2010-01-11:2010-01-20,2010-01-21:2010-01-31,2010-02-01:2010-02-11");
        oneBand.setInternalRegionNames("r1", "r2", "r3");
        oneBand.setWriteSeparateHistogramFile(false);
        RegionAnalysis ra = new RegionAnalysis(dateRanges, oneBand, writer);
        ra.startRegion("r1");
        ra.addData(dateFormat.parse("2010-01-15 10:00:00").getTime(), 7, new float[][]{{1, 2, 3}});
        ra.addData(dateFormat.parse("2010-01-15 11:00:00").getTime(), 8, new float[][]{{4, 5, 6}});
        ra.addData(dateFormat.parse("2010-01-15 12:00:00").getTime(), 9, new float[][]{{7, 8, 9}});
        ra.addData(dateFormat.parse("2010-02-03 12:00:00").getTime(), 11, new float[][]{{3, 4, 5}});
        ra.endRegion();
        ra.startRegion("r2");
        ra.endRegion();
        ra.startRegion("r3");
        ra.endRegion();
        ra.close();

        Set<String> keySet = writer.writerMap.keySet();
        assertEquals(1, keySet.size());
        assertTrue(keySet.contains("region-statistics.csv"));

        String expected = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\tb1_belowHistogram\tb1_aboveHistogram\tb1_numBins\tb1_lowValue\tb1_highValue\tb1_bin_0\tb1_bin_1\tb1_bin_2\tb1_bin_3\tb1_bin_4\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r1\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t3\t24\t9\t1.0\t9.0\t5.0\t2.5819888974716116\t4.147166274396913\t1.0\t2.5\t5.0\t7.5\t9.0\t0\t0\t5\t0.0\t10.0\t1\t2\t2\t2\t2\n" +
                "r1\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r1\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t1\t11\t3\t3.0\t5.0\t4.0\t0.8164965809277268\t3.9148676411688634\t3.0\t3.0\t4.0\t5.0\t5.0\t0\t0\t5\t0.0\t10.0\t0\t1\t2\t0\t0\n" +
                "r2\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r2\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r2\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r2\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r3\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r3\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r3\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r3\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n";
        String actual = writer.writerMap.get("region-statistics.csv").toString();
        assertEquals(expected, actual);
    }

    @Test
    public void test_many_many_perRegion() throws Exception {
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10,2010-01-11:2010-01-20,2010-01-21:2010-01-31,2010-02-01:2010-02-11");
        oneBand.setInternalRegionNames("r1", "r2", "r3");
        oneBand.setWriteStatisticsFilePerRegion(true);

        RegionAnalysis ra = new RegionAnalysis(dateRanges, oneBand, writer);
        ra.startRegion("r1");
        ra.addData(dateFormat.parse("2010-01-15 10:00:00").getTime(), 7, new float[][]{{1, 2, 3}});
        ra.addData(dateFormat.parse("2010-01-15 11:00:00").getTime(), 8, new float[][]{{4, 5, 6}});
        ra.addData(dateFormat.parse("2010-01-15 12:00:00").getTime(), 9, new float[][]{{7, 8, 9}});
        ra.addData(dateFormat.parse("2010-02-03 12:00:00").getTime(), 11, new float[][]{{3, 4, 5}});
        ra.endRegion();
        ra.startRegion("r2");
        ra.endRegion();
        ra.startRegion("r3");
        ra.endRegion();
        ra.close();

        Set<String> keySet = writer.writerMap.keySet();
        assertEquals(6, keySet.size());
        assertTrue(keySet.contains("region-r1-statistics.csv"));
        assertTrue(keySet.contains("region-r2-statistics.csv"));
        assertTrue(keySet.contains("region-r3-statistics.csv"));
        assertTrue(keySet.contains("region-r1-histogram-b1.csv"));
        assertTrue(keySet.contains("region-r2-histogram-b1.csv"));
        assertTrue(keySet.contains("region-r3-histogram-b1.csv"));

        String expected = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r1\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t3\t24\t9\t1.0\t9.0\t5.0\t2.5819888974716116\t4.147166274396913\t1.0\t2.5\t5.0\t7.5\t9.0\n" +
                "r1\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r1\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t1\t11\t3\t3.0\t5.0\t4.0\t0.8164965809277268\t3.9148676411688634\t3.0\t3.0\t4.0\t5.0\t5.0\n" ;
        String actual = writer.writerMap.get("region-r1-statistics.csv").toString();
        assertEquals(expected, actual);

        expected = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\n" +
                "r2\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r2\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r2\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r2\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n";
        actual = writer.writerMap.get("region-r2-statistics.csv").toString();
        assertEquals(expected, actual);

        expected = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_count\tb1_min\tb1_max\tb1_arithMean\tb1_sigma\tb1_geomMean\tb1_p5\tb1_p25\tb1_p50\tb1_p75\tb1_p95\n" +
                "r3\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r3\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r3\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n" +
                "r3\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t0\t0\t0\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\n";
        actual = writer.writerMap.get("region-r3-statistics.csv").toString();
        assertEquals(expected, actual);

        expected = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_belowHistogram\tb1_aboveHistogram\tb1_numBins\tb1_lowValue\tb1_highValue\tb1_bin_0\tb1_bin_1\tb1_bin_2\tb1_bin_3\tb1_bin_4\n" +
                "r1\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r1\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t3\t24\t0\t0\t5\t0.0\t10.0\t1\t2\t2\t2\t2\n" +
                "r1\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r1\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t1\t11\t0\t0\t5\t0.0\t10.0\t0\t1\t2\t0\t0\n";
        actual = writer.writerMap.get("region-r1-histogram-b1.csv").toString();
        assertEquals(expected, actual);

        expected = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_belowHistogram\tb1_aboveHistogram\tb1_numBins\tb1_lowValue\tb1_highValue\tb1_bin_0\tb1_bin_1\tb1_bin_2\tb1_bin_3\tb1_bin_4\n" +
                "r2\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r2\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r2\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r2\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n";
        actual = writer.writerMap.get("region-r2-histogram-b1.csv").toString();
        assertEquals(expected, actual);

        expected = "RegionId\tTimeWindow_start\tTimeWindow_end\tnumPasses\tnumObs\tb1_belowHistogram\tb1_aboveHistogram\tb1_numBins\tb1_lowValue\tb1_highValue\tb1_bin_0\tb1_bin_1\tb1_bin_2\tb1_bin_3\tb1_bin_4\n" +
                "r3\t2010-01-01 00:00:00\t2010-01-10 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r3\t2010-01-11 00:00:00\t2010-01-20 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r3\t2010-01-21 00:00:00\t2010-01-31 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n" +
                "r3\t2010-02-01 00:00:00\t2010-02-11 23:59:59\t0\t0\t0\t0\t5\t0.0\t10.0\t0\t0\t0\t0\t0\n";
        actual = writer.writerMap.get("region-r3-histogram-b1.csv").toString();
        assertEquals(expected, actual);
    }

    private static class InMemWriterFactory implements WriterFactory {

        Map<String, Writer> writerMap;

        @Override
        public Writer createWriter(String fileName) throws IOException {
            if (writerMap == null) {
                writerMap = new HashMap<>();
            }
            StringWriter stringWriter = new StringWriter();
            writerMap.put(fileName, stringWriter);
            return stringWriter;
        }
    }

}