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

import org.apache.hadoop.fs.Path;
import org.esa.snap.core.datamodel.ProductData;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.*;


/**
 * @author MarcoZ
 * @author Norman
 */
public class AbstractFileSystemServiceTest {

    @Test
    public void testGetCommonPathPrefix() throws Exception {
        assertEquals("", AbstractFileSystemService.getCommonPathPrefix(Collections.emptyList()));
        assertEquals("", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("")));
        assertEquals("", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList(".*")));
        assertEquals("", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("abc.*")));
        assertEquals("abc", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("abc")));
        assertEquals("abc", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("abc/.*")));
        assertEquals("abc", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("abc/efg.*")));
        assertEquals("ab/cd", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("ab/cd/MER_.*.N1")));

        assertEquals("ab/cd", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("ab/cd/ef", "ab/cd/gh")));
        assertEquals("ab/cd", AbstractFileSystemService.getCommonPathPrefix(
                Arrays.asList("ab/cd/gh", "ab/cd/ef", "ab/cd/ef/gh")));
        assertEquals("ab/cd", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("ab/cd/.*", "ab/cd/.*")));

        assertEquals("", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("", "bcd")));
        assertEquals("", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("a", "")));
        assertEquals("ab", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("ab/cd", "ab/ce")));


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

    @Test
    public void testPathIsAbsolute() {
        assertTrue(new Path("/a").isAbsolute());
        assertFalse(new Path("a").isAbsolute());
        assertTrue(new Path("file:///a").isAbsolute());
        assertFalse(new Path("file://a").isAbsolute());
        assertTrue(new Path("hdfs:///a").isAbsolute());
        assertFalse(new Path("hdfs://a").isAbsolute());
    }

    @Test
    public void testGetRegexpForPathGlob() throws Exception {
        // empty
        testGetRegexpForPathGlob("", "^$", "", true);
        testGetRegexpForPathGlob("", "^$", "abc", false);

        // *
        testGetRegexpForPathGlob("*", "^[^/]*$", "", true);
        testGetRegexpForPathGlob("*", "^[^/]*$", "abc", true);
        testGetRegexpForPathGlob("abc/*", "^abc/[^/]*$", "abc", false);
        testGetRegexpForPathGlob("abc/*", "^abc/[^/]*$", "abc/def", true);
        testGetRegexpForPathGlob("abc/*", "^abc/[^/]*$", "abc/def/ghi", false);
        testGetRegexpForPathGlob("abc/*/def", "^abc/[^/]*/def$", "abc/ghi/def", true);
        testGetRegexpForPathGlob("abc/*/def", "^abc/[^/]*/def$", "abc/def/ghi", false);
        testGetRegexpForPathGlob("abc/*/def", "^abc/[^/]*/def$", "abc/def", false);

        // '*' and '.'
        testGetRegexpForPathGlob("abc/*.zip", "^abc/[^/]*\\.zip$", "abc/efg.zip", true);
        testGetRegexpForPathGlob("abc/*.zip", "^abc/[^/]*\\.zip$", "abc/efgzip", false);
        testGetRegexpForPathGlob("abc/*.zip", "^abc/[^/]*\\.zip$", "abc/efg/.zip", false);

        // ?
        testGetRegexpForPathGlob("abc/?.zip", "^abc/[^/]{1}\\.zip$", "abc/a.zip", true);
        testGetRegexpForPathGlob("abc/?.zip", "^abc/[^/]{1}\\.zip$", "abc/2.zip", true);
        testGetRegexpForPathGlob("abc/?.zip", "^abc/[^/]{1}\\.zip$", "abc/.zip", false);
        testGetRegexpForPathGlob("abc/?.zip", "^abc/[^/]{1}\\.zip$", "abc/zip", false);
        testGetRegexpForPathGlob("abc?efg", "^abc[^/]{1}efg$", "abcdefg", true);
        testGetRegexpForPathGlob("abc?efg", "^abc[^/]{1}efg$", "abc.efg", true);
        testGetRegexpForPathGlob("abc?efg", "^abc[^/]{1}efg$", "abc/efg", false);
        testGetRegexpForPathGlob("abc?efg", "^abc[^/]{1}efg$", "abcefg", false);

        // **
        testGetRegexpForPathGlob("abc/**/ghi", "^abc/.*/ghi$", "abc/def/ghi", true);
        testGetRegexpForPathGlob("abc/**/ghi", "^abc/.*/ghi$", "abc/def/def/ghi", true);
        testGetRegexpForPathGlob("abc/**/ghi", "^abc/.*/ghi$", "abc/def/uvw/xyz/ghi", true);
        testGetRegexpForPathGlob("abc/**/ghi", "^abc/.*/ghi$", "abc/def/uvw/ghi/ghi", true);
        testGetRegexpForPathGlob("abc/**/ghi", "^abc/.*/ghi$", "abc/ghi", false);
        testGetRegexpForPathGlob("abc/**/ghi", "^abc/.*/ghi$", "abc", false);
        testGetRegexpForPathGlob("abc/**/ghi", "^abc/.*/ghi$", "abc/", false);
        testGetRegexpForPathGlob("**/def/ghi", "^.*/def/ghi$", "abc/def/ghi", true);
        testGetRegexpForPathGlob("**/def/ghi", "^.*/def/ghi$", "abc/cba/uvw/def/ghi", true);
        testGetRegexpForPathGlob("**/def/ghi", "^.*/def/ghi$", "def/ghi", false);
        testGetRegexpForPathGlob("abc/**", "^abc/.*$", "abc/def", true);
        testGetRegexpForPathGlob("abc/**", "^abc/.*$", "abc/def/ghi", true);
        testGetRegexpForPathGlob("abc/**", "^abc/.*$", "abc", false);
        testGetRegexpForPathGlob("abc/**", "^abc/.*$", "abc/.", true);

        // '**', '*' and '?'
        testGetRegexpForPathGlob("/eodata/**/MER__RR_?P*.N1", "^/eodata/.*/MER__RR_[^/]{1}P[^/]*\\.N1$",
                                 "/eodata/r03/2005/05/21/MER__RR_2PACR200505211025.N1", true);
        testGetRegexpForPathGlob("/eodata/**/MER__RR_?P*.N1", "^/eodata/.*/MER__RR_[^/]{1}P[^/]*\\.N1$",
                                 "/eodata/r03/2005/05/21/MER__RR_2BACR200505211025.N1", false);
        testGetRegexpForPathGlob("/eodata/**/MER__RR_?P*.N1", "^/eodata/.*/MER__RR_[^/]{1}P[^/]*\\.N1$",
                                 "/eodata/MER__RR_2BACR200505211025.N1", false);

    }

    private void testGetRegexpForPathGlob(String testGlob, String expectedRegexp, String testPath,
                                          boolean expectedMatch) {
        String regex = DefaultInventoryService.getRegexpForPathGlob(testGlob);
        assertEquals(expectedRegexp, regex);
        assertEquals(expectedMatch, Pattern.matches(regex, testPath));
    }

    @Test
    public void testPatternMatching() throws Exception {
        String regex = "foo/[^_.].*";
        assertTrue("foo/abc".matches(regex));
        assertFalse("foo/_abc".matches(regex));
        assertFalse("foo/.abc".matches(regex));

        regex = ".*\\.(N1|nc|hdf|seq)$";
        assertTrue("MER.N1".matches(regex));
        assertTrue("MER.hdf".matches(regex));
        assertTrue("MER.nc".matches(regex));
        assertTrue("MER.seq".matches(regex));
        assertTrue("MER.seq".matches(regex));
        assertFalse("MER.seq.index".matches(regex));
    }
}
