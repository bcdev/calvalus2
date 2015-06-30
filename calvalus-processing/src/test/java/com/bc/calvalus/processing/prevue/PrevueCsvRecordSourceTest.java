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

package com.bc.calvalus.processing.prevue;

import com.bc.calvalus.processing.ma.CsvRecordSource;
import com.bc.calvalus.processing.ma.Header;
import com.bc.calvalus.processing.ma.Record;
import org.esa.snap.framework.datamodel.GeoPos;
import org.junit.Test;

import java.io.StringReader;
import java.util.Iterator;

import static org.junit.Assert.*;


public class PrevueCsvRecordSourceTest {

    @Test
    public void testSimpleCsv() throws Exception {
        final String CSV = ""
                + "# Test CSV\n"
                + "ID\tLAT\tLONG\n"
                + "16\t53.1\t13.6\n"
                + "17\t53.3\t13.4\n"
                + "18\t53.1\t13.5\n";


        CsvRecordSource recordSource = new CsvRecordSource(new StringReader(CSV), null);
        Header header = recordSource.getHeader();
        assertNotNull(header);
        assertNotNull(header.getAttributeNames());
        assertArrayEquals(new String[]{"ID", "LAT", "LONG"}, header.getAttributeNames());
        assertEquals(true, header.hasLocation());
        assertEquals(false, header.hasTime());

        Iterable<Record> records = recordSource.getRecords();
        assertNotNull(records);

        Iterator<Record> iterator = records.iterator();
        assertNotNull(iterator);

        assertTrue(iterator.hasNext());
        Record rec1 = iterator.next();
        assertNotNull(rec1);
        assertArrayEquals(new Object[]{(double) 16, 53.1, 13.6}, rec1.getAttributeValues());
        assertEquals(new GeoPos(53.1F, 13.6F), rec1.getLocation());

        assertTrue(iterator.hasNext());
        Record rec2 = iterator.next();
        assertNotNull(rec2);
        assertArrayEquals(new Object[]{(double) 17, 53.3, 13.4}, rec2.getAttributeValues());
        assertEquals(new GeoPos(53.3F, 13.4F), rec2.getLocation());

        assertTrue(iterator.hasNext());
        Record rec3 = iterator.next();
        assertNotNull(rec3);
        assertArrayEquals(new Object[]{(double) 18, 53.1, 13.5}, rec3.getAttributeValues());
        assertEquals(new GeoPos(53.1F, 13.5F), rec3.getLocation());
    }

}
