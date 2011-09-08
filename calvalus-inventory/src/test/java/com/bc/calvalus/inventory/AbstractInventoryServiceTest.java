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

package com.bc.calvalus.inventory;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;


/**
 * @author MarcoZ
 */
public class AbstractInventoryServiceTest {

    @Test
    public void testRegexp() throws Exception {
        assertEquals(true, glob("", ""));
    }

    private boolean glob(String glob, String filePath) {
        Pattern pattern = Pattern.compile(glob);
        Matcher matcher = pattern.matcher(filePath);
        return matcher.matches();
    }

    @Test
    public void testGetCommonPathPrefix() throws Exception {
        assertEquals("", AbstractInventoryService.getCommonPathPrefix(Collections.<String>emptyList()));
        assertEquals("", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("")));
        assertEquals("", AbstractInventoryService.getCommonPathPrefix(Arrays.asList(".*")));
        assertEquals("", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("abc.*")));
        assertEquals("abc", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("abc")));
        assertEquals("abc", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("abc/.*")));
        assertEquals("abc", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("abc/efg.*")));
        assertEquals("ab/cd", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("ab/cd/MER_.*.N1")));

        assertEquals("ab/cd", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("ab/cd/ef", "ab/cd/gh")));
        assertEquals("ab/cd", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("ab/cd/gh", "ab/cd/ef", "ab/cd/ef/gh")));
        assertEquals("ab/cd", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("ab/cd/.*", "ab/cd/.*")));

        assertEquals("", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("", "bcd")));
        assertEquals("", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("a", "")));
        assertEquals("ab", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("ab/cd", "ab/ce")));


        /* Talk with MarcoZ about the use of these tests:
        assertEquals("ab", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("abc", "abd")));
        assertEquals("ab", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("abc", "ab")));
        assertEquals("a", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("abc", "acd")));
        assertEquals("", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("abc", "bcd")));
        assertEquals("", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("", "bcd")));
        assertEquals("", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("a", "")));
        assertEquals("ab", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("ab*cd", "ab*cd")));
        assertEquals("ab", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("ab/cd", "ab/ce")));
        assertEquals("ab/cd", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("ab/cd/*", "ab/cd/*")));
        assertEquals("ab/cd", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("ab/cd/*")));
        */
    }
}
