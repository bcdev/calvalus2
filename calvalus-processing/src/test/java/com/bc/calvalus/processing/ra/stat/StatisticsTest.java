/*
 * Copyright (C) 2017 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.ra.stat;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class StatisticsTest {

    @Test
    public void test_header() throws Exception {
        String[] expected = {"b1_numValid", "b1_min", "b1_max", "b1_arithMean", "b1_sigma", "b1_geomMean",
                "b1_p05", "b1_p25", "b1_p50", "b1_p75", "b1_p95"};
        assertArrayEquals(expected, new Statistics().getStatisticsHeaders("b1").toArray());
        assertArrayEquals(expected, new Statistics(3, 0.0, 1.0).getStatisticsHeaders("b1").toArray());

        expected = new String[]{};
        assertArrayEquals(expected, new Statistics().getHistogramHeaders("b1").toArray());

        expected = new String[]{"a1_belowHistogram", "a1_aboveHistogram", "a1_numBins", "a1_lowValue", "a1_highValue", "a1_bin_0", "a1_bin_1", "a1_bin_2"};
        assertArrayEquals(expected, new Statistics(3, 0.0, 1.0).getHistogramHeaders("a1").toArray());

        expected = new String[]{"a1_belowHistogram", "a1_aboveHistogram", "a1_numBins", "a1_lowValue", "a1_highValue", "a1_bin_0", "a1_bin_1", "a1_bin_2", "a1_bin_3", "a1_bin_4"};
        assertArrayEquals(expected, new Statistics(5, 0.0, 1.0).getHistogramHeaders("a1").toArray());
    }

    @Test
    public void test_zero() throws Exception {
        String[] stat = {"0", "NaN", "NaN", "NaN", "NaN", "NaN", "NaN", "NaN", "NaN", "NaN", "NaN"};
        String[] histo = {};
        testStat(new Statistics(), stat, histo);

        histo = new String[]{"0", "0", "3", "0.0", "1.0", "0", "0", "0"};
        testStat(new Statistics(3, 0.0, 1.0), stat, histo);
    }

    @Test
    public void test_one() throws Exception {
        float[] samples = {42f};
        String[] stat = {"1", "42.0", "42.0", "42.0", "0.0", "42.00000000000001", "42.0", "42.0", "42.0", "42.0", "42.0"};
        String[] histo = {};
        testStat(new Statistics(), stat, histo, samples);

        histo = new String[]{"0", "0", "4", "0.0", "100.0", "0", "1", "0", "0"};
        testStat(new Statistics(4, 0.0, 100.0), stat, histo, samples);
    }

    @Test
    public void test_two() throws Exception {
        float[] samples = {42f, 44f};
        String[] stat = {"2", "42.0", "44.0", "43.0", "1.0", "42.988370520409354", "42.0", "42.0", "43.0", "44.0", "44.0"};
        String[] histo = {};
        testStat(new Statistics(), stat, histo, samples);

        histo = new String[]{"0", "0", "4", "0.0", "100.0", "0", "2", "0", "0"};
        testStat(new Statistics(4, 0.0, 100.0), stat, histo, samples);
    }

    @Test
    public void test_many() throws Exception {
        float[] samples = {1f, 2f, 3f, 4f, 5f, 6f, 7f};
        String[] stat = {"7", "1.0", "7.0", "4.0", "2.0", "3.3800151591412964", "1.0", "2.0", "4.0", "6.0", "7.0"};
        String[] histo = {};
        testStat(new Statistics(), stat, histo, samples);

        histo = new String[]{"0", "0", "4", "0.0", "10.0", "2", "2", "3", "0"};
        testStat(new Statistics(4, 0.0, 10.0), stat, histo, samples);

        float[] samples2 = {-1f, Float.NaN, 1f, 2f, 3f, 4f, Float.NaN, 5f, 6f, 7f, 11f};
        String[] stat2 = {"9", "-1.0", "11.0", "4.222222222222222", "3.3591592128513272", "3.9172194543068826", "-1.0", "0.0", "3.0", "6.0", "11.0"};
        String[] histo2 = {"1", "1", "4", "0.0", "10.0", "2", "2", "3", "0"};
        testStat(new Statistics(4, 0.0, 10.0), stat2, histo2, samples2);
    }

    private static void testStat(Statistics stat, String[] recordsStat, String[] recordsHisto, float... samples) {
        stat.process(samples);
        Object[] stats = stat.getStatisticsRecords().toArray();
        //System.out.println("StatisticsRecords = " + Arrays.toString(stats));
        assertArrayEquals("statitics", recordsStat, stats);
        Object[] histo = stat.getHistogramRecords().toArray();
        //System.out.println("HistogramRecords = " + Arrays.toString(histo));
        assertArrayEquals("histogram", recordsHisto, histo);
    }
}