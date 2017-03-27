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

public class AccumulatorTest {

    @Test
    public void test() throws Exception {
        Accumulator acc = new Accumulator();
        assertArrayEquals(new float[0], acc.getValues(), 1E-5f);

        acc.accumulate();
        assertArrayEquals(new float[0], acc.getValues(), 1E-5f);

        acc.accumulate(1, 2, 3);
        assertArrayEquals(new float[]{1, 2, 3}, acc.getValues(), 1E-5f);
        acc.accumulate(4, 5);
        assertArrayEquals(new float[]{1, 2, 3, 4, 5}, acc.getValues(), 1E-5f);
    }
}