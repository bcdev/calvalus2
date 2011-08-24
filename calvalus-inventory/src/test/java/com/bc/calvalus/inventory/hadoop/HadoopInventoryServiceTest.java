/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.inventory.hadoop;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;


/**
 * @author MarcoZ
 */
public class HadoopInventoryServiceTest {

    @Test
    public void testGetCommonPathPrefix() throws Exception {
        assertEquals("ab", HadoopInventoryService.getCommonPathPrefix(Arrays.asList("abc", "abd")));
        assertEquals("ab", HadoopInventoryService.getCommonPathPrefix(Arrays.asList("abc", "ab")));
        assertEquals("a", HadoopInventoryService.getCommonPathPrefix(Arrays.asList("abc", "acd")));
        assertEquals("", HadoopInventoryService.getCommonPathPrefix(Arrays.asList("abc", "bcd")));
        assertEquals("", HadoopInventoryService.getCommonPathPrefix(Arrays.asList("", "bcd")));
        assertEquals("", HadoopInventoryService.getCommonPathPrefix(Arrays.asList("a", "")));
        assertEquals("ab", HadoopInventoryService.getCommonPathPrefix(Arrays.asList("ab*cd", "ab*cd")));
        assertEquals("ab", HadoopInventoryService.getCommonPathPrefix(Arrays.asList("ab/cd", "ab/ce")));
        assertEquals("ab/cd", HadoopInventoryService.getCommonPathPrefix(Arrays.asList("ab/cd/*", "ab/cd/*")));
    }
}
