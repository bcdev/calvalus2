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

import static org.junit.Assert.*;

public class StatiticsComputerTest {

    @Test
    public void test_header() throws Exception {
        String[] expected = {"b1_count",
                "b1_min", "b1_max", "b1_arithMean", "b1_sigma", "b1_geomMean",
                "b1_p5", "b1_p25", "b1_p50", "b1_p75", "b1_p95"};
        assertArrayEquals(expected, StatiticsComputer.getHeader("b1").toArray());
    }

    @Test
    public void test_empty() throws Exception {
        StatiticsComputer stx = new StatiticsComputer("b1");
        String[] expected = {"0",
                "NaN", "NaN", "NaN", "NaN", "NaN",
                "NaN", "NaN", "NaN", "NaN", "NaN"};
        assertArrayEquals(expected, stx.getStats().toArray());
    }

    @Test
    public void test_one() throws Exception {
        StatiticsComputer stx = new StatiticsComputer("b1", 42f);
        String[] expected = {"1",
                "42.0", "42.0", "42.0", "0.0", "42.0",
                "42.0", "42.0", "42.0", "42.0", "42.0"};
        assertArrayEquals(expected, stx.getStats().toArray());
    }

    @Test
    public void test_two() throws Exception {
        StatiticsComputer stx = new StatiticsComputer("b1", 42f, 44f);
        String[] expected = {"2",
                "42.0", "44.0", "43.0", "1.0", "42.988370520409354",
                "42.0", "42.0", "43.0", "44.0", "44.0"};
        assertArrayEquals(expected, stx.getStats().toArray());
    }

    @Test
    public void test_many() throws Exception {
        StatiticsComputer stx = new StatiticsComputer("b1", 1f, 2f, 3f, 4f, 5f, 6f, 7f);
        String[] expected = {"7",
                "1.0", "7.0", "4.0", "2.0", "3.3800151591412964",
                "1.0", "2.0", "4.0", "6.0", "7.0"};
        assertArrayEquals(expected, stx.getStats().toArray());
    }
}