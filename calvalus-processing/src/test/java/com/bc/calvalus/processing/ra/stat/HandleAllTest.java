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

public class HandleAllTest {

    @Test
    public void test() throws Exception {
        HandleAll three = new HandleAll(3);
        assertEquals(-1, three.current());
        assertArrayEquals(new int[]{0,1,2}, three.remaining());
        assertArrayEquals(new int[]{0,1,2}, three.remaining());
        assertArrayEquals(new int[]{}, three.next(0));
        assertEquals(0, three.current());
        assertArrayEquals(new int[]{1}, three.next(2));
        assertEquals(2, three.current());
        try {
            three.next(3);
            fail();
        } catch (IllegalArgumentException iae) {
            assertEquals("nextIndex(3) >= numItems(3)", iae.getMessage());
        }
        assertArrayEquals(new int[]{}, three.remaining());

        three.reset();
        assertEquals(-1, three.current());
        assertArrayEquals(new int[]{0,1}, three.next(2));
        assertEquals(2, three.current());
    }
}