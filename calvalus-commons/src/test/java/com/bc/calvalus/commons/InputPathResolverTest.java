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

package com.bc.calvalus.commons;

import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import static org.junit.Assert.*;

public class InputPathResolverTest {

    @Test
    public void testThatInputIsOutputWithSimpleNameAndNoVars() throws ParseException {
        List<String> pathGlobs = InputPathResolver.getInputPathPatterns("foo", null, null, null);
        assertNotNull(pathGlobs);
        assertEquals(1, pathGlobs.size());
        assertEquals("foo", pathGlobs.get(0));
    }

    @Test
    public void testThatInputPatternsCanHaveMultipleEntries() throws ParseException {
        List<String> pathGlobs = InputPathResolver.getInputPathPatterns("foo,bar,baz", null, null, null);
        assertNotNull(pathGlobs);
        assertEquals(3, pathGlobs.size());
        assertEquals("foo", pathGlobs.get(0));
        assertEquals("bar", pathGlobs.get(1));
        assertEquals("baz", pathGlobs.get(2));
    }


    @Test
    public void testThatInputIsOutputWithSlashesNameAndNoVars() throws ParseException {
        List<String> pathGlobs = InputPathResolver.getInputPathPatterns("/foo/", null, null, null);
        assertNotNull(pathGlobs);
        assertEquals(1, pathGlobs.size());
        assertEquals("/foo/", pathGlobs.get(0));
    }

    @Test
    public void testThatRegionStringIsReplaced() throws ParseException {
        List<String> pathGlobs = InputPathResolver.getInputPathPatterns("/foo/${region}/.*.N1", null, null, "northsea");
        assertNotNull(pathGlobs);
        assertEquals(1, pathGlobs.size());
        assertEquals("/foo/northsea/.*.N1", pathGlobs.get(0));
    }

    @Test
    public void testThatRegionStringIsReplacedWithStarWhenNotGiven() throws ParseException {
        List<String> pathGlobs = InputPathResolver.getInputPathPatterns("/foo/${region}/.*.N1", null, null, null);
        assertNotNull(pathGlobs);
        assertEquals(1, pathGlobs.size());
        assertEquals("/foo/.*/.*.N1", pathGlobs.get(0));
    }

    @Test
    public void testThatRegionStringIsReplacedMultipleTimes() throws ParseException {
        List<String> pathGlobs = InputPathResolver.getInputPathPatterns("/foo/${region}/bar/${region}/.*.N1", null,
                                                                        null, "northsea");
        assertNotNull(pathGlobs);
        assertEquals(1, pathGlobs.size());
        assertEquals("/foo/northsea/bar/northsea/.*.N1", pathGlobs.get(0));
    }

    @Test
    public void testThatDatePartsAreReplaced() throws ParseException {
        List<String> pathGlobs = InputPathResolver.getInputPathPatterns("/foo/${yyyy}/${MM}/${dd}/.*.N1",
                                                                        date("2005-12-30"), date("2006-01-02"), null);
        assertNotNull(pathGlobs);
        assertEquals(4, pathGlobs.size());
        assertEquals("/foo/2005/12/30/.*.N1", pathGlobs.get(0));
        assertEquals("/foo/2005/12/31/.*.N1", pathGlobs.get(1));
        assertEquals("/foo/2006/01/01/.*.N1", pathGlobs.get(2));
        assertEquals("/foo/2006/01/02/.*.N1", pathGlobs.get(3));
    }

    @Test
    public void testThatDatePartsAreReplacedWhenNotGiven() throws ParseException {
        List<String> pathGlobs = InputPathResolver.getInputPathPatterns("/foo/${yyyy}/${MM}/${dd}/.*.N1", null, null,
                                                                        null);
        assertNotNull(pathGlobs);
        assertEquals(1, pathGlobs.size());
        assertEquals("/foo/.*/.*/.*/.*.N1", pathGlobs.get(0));
    }

    @Test
    public void testThatGlobsAreUnique() throws ParseException {
        List<String> pathGlobs = InputPathResolver.getInputPathPatterns("/foo/${yyyy}/${MM}/.*.N1", date("2005-01-01"),
                                                                        date("2005-01-03"), null);
        assertNotNull(pathGlobs);
        assertEquals(1, pathGlobs.size());
        assertEquals("/foo/2005/01/.*.N1", pathGlobs.get(0));
    }

    @Test
    public void testThatDatePartsAreConcatenated() throws ParseException {
        List<String> pathGlobs = InputPathResolver.getInputPathPatterns(
                "/foo/MER_RR__1P\\p{Upper}+${yyyy}${MM}${dd}*.N1", date("2005-01-01"), date("2005-01-03"), null);
        assertNotNull(pathGlobs);
        assertEquals(3, pathGlobs.size());
        assertEquals("/foo/MER_RR__1P\\p{Upper}+20050101*.N1", pathGlobs.get(0));
        assertEquals("/foo/MER_RR__1P\\p{Upper}+20050102*.N1", pathGlobs.get(1));
        assertEquals("/foo/MER_RR__1P\\p{Upper}+20050103*.N1", pathGlobs.get(2));
    }

    @Test
    public void testContainsDateVariables() throws Exception {
        assertFalse(InputPathResolver.containsDateVariables(""));
        assertFalse(InputPathResolver.containsDateVariables("foo"));
        assertTrue(InputPathResolver.containsDateVariables("/foo/${yyyy}/${MM}/${dd}/.*.N1"));
        assertTrue(InputPathResolver.containsDateVariables("/foo/${yyyy}/${MM}/.*.N1"));
        assertTrue(InputPathResolver.containsDateVariables("/foo/${yyyy}.*.N1"));

    }

    private Date date(String dateAsString) throws ParseException {
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        final Calendar calendar = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ENGLISH);
        dateFormat.setCalendar(calendar);
        return dateFormat.parse(dateAsString);
    }

}
