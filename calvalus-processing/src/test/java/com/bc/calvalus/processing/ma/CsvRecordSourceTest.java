package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.ProductData;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.StringReader;
import java.text.DateFormat;
import java.util.Iterator;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class CsvRecordSourceTest {
    @Test
    public void testSimpleCsv() throws Exception {
        final String CSV = ""
                + "# Test CSV\n"
                + "ID\tLAT\tLONG\tTIME\tSITE\tCHL\tFLAG\n"
                + "16\t53.1\t13.6\t03.04.2003\tA\t0.5\t1\n"
                + "17\t53.3\t13.4\t08.04.2003\tA\t0.9\t0\n"
                + "18\t53.1\t13.5\t11.04.2003\tA\t0.4\t1\n";


        DateFormat dateFormat = ProductData.UTC.createDateFormat("dd.MM.yyyy");

        CsvRecordSource recordSource = new CsvRecordSource(new StringReader(CSV), dateFormat);
        Header header = recordSource.getHeader();
        assertNotNull(header);
        assertNotNull(header.getAttributeNames());
        assertArrayEquals(new String[]{"ID", "LAT", "LONG", "TIME", "SITE", "CHL", "FLAG"},
                          header.getAttributeNames());
        assertEquals(true, header.hasLocation());
        assertEquals(true, header.hasTime());

        Iterable<Record> records = recordSource.getRecords();
        assertNotNull(records);

        Iterator<Record> iterator = records.iterator();
        assertNotNull(iterator);

        assertTrue(iterator.hasNext());
        Record rec1 = iterator.next();
        assertNotNull(rec1);
        assertArrayEquals(new Object[]{(double) 16, 53.1, 13.6, dateFormat.parse("03.04.2003"), "A", 0.5, (double) 1},
                          rec1.getAttributeValues());
        assertEquals(new GeoPos(53.1F, 13.6F), rec1.getLocation());
        assertEquals(dateFormat.parse("03.04.2003"), rec1.getTime());

        assertTrue(iterator.hasNext());
        Record rec2 = iterator.next();
        assertNotNull(rec2);
        assertArrayEquals(new Object[]{(double) 17, 53.3, 13.4, dateFormat.parse("08.04.2003"), "A", 0.9, (double) 0},
                          rec2.getAttributeValues());
        assertEquals(new GeoPos(53.3F, 13.4F), rec2.getLocation());
        assertEquals(dateFormat.parse("08.04.2003"), rec2.getTime());

        assertTrue(iterator.hasNext());
        Record rec3 = iterator.next();
        assertNotNull(rec3);
        assertArrayEquals(new Object[]{(double) 18, 53.1, 13.5, dateFormat.parse("11.04.2003"), "A", 0.4, (double) 1},
                          rec3.getAttributeValues());
        assertEquals(new GeoPos(53.1F, 13.5F), rec3.getLocation());
        assertEquals(dateFormat.parse("11.04.2003"), rec3.getTime());
    }

    @Test
    public void testSimpleCsvWithMissingValues() throws Exception {
        final String CSV = ""
                + "\n"
                + "# Test CSV\n"
                + "\n"
                + "ID\tLAT\tLONG\tTIME\tSITE\tCHL\tFLAG\n"
                + "16\t53.1\t13.6\t03.04.2003\t\t0.5\t1\n"
                + "17\t53.3\t13.4\t08.04.2003\t\t\t\n"
                + "18\t53.1\t13.5\t11.04.2003\tA\t0.4\t\n";


        DateFormat dateFormat = ProductData.UTC.createDateFormat("dd.MM.yyyy");

        CsvRecordSource recordSource = new CsvRecordSource(new StringReader(CSV), dateFormat);
        Header header = recordSource.getHeader();
        assertNotNull(header);
        assertNotNull(header.getAttributeNames());
        assertArrayEquals(new String[]{"ID", "LAT", "LONG", "TIME", "SITE", "CHL", "FLAG"},
                          header.getAttributeNames());
        assertEquals(true, header.hasLocation());
        assertEquals(true, header.hasTime());

        Iterable<Record> records = recordSource.getRecords();
        assertNotNull(records);

        Iterator<Record> iterator = records.iterator();
        assertNotNull(iterator);

        assertTrue(iterator.hasNext());
        Record rec1 = iterator.next();
        assertNotNull(rec1);
        assertArrayEquals(new Object[]{(double) 16, 53.1, 13.6, dateFormat.parse("03.04.2003"), null, 0.5, (double) 1},
                          rec1.getAttributeValues());
        assertEquals(new GeoPos(53.1F, 13.6F), rec1.getLocation());
        assertEquals(dateFormat.parse("03.04.2003"), rec1.getTime());

        assertTrue(iterator.hasNext());
        Record rec2 = iterator.next();
        assertNotNull(rec2);
        assertArrayEquals(new Object[]{(double) 17, 53.3, 13.4, dateFormat.parse("08.04.2003"), null, null, null},
                          rec2.getAttributeValues());
        assertEquals(new GeoPos(53.3F, 13.4F), rec2.getLocation());
        assertEquals(dateFormat.parse("08.04.2003"), rec2.getTime());

        assertTrue(iterator.hasNext());
        Record rec3 = iterator.next();
        assertNotNull(rec3);
        assertArrayEquals(new Object[]{(double) 18, 53.1, 13.5, dateFormat.parse("11.04.2003"), "A", 0.4, null},
                          rec3.getAttributeValues());
        assertEquals(new GeoPos(53.1F, 13.5F), rec3.getLocation());
        assertEquals(dateFormat.parse("11.04.2003"), rec3.getTime());
    }

    @Test
    public void testRealLifeCsv() throws Exception {
        InputStreamReader reader = new InputStreamReader(getClass().getResourceAsStream("cc-matchup-test-insitu.csv"));
        CsvRecordSource recordSource = new CsvRecordSource(reader, TextUtils.DEFAULT_DATE_FORMAT);

        Header header = recordSource.getHeader();
        assertNotNull(header);
        assertNotNull(header.getAttributeNames());
        assertArrayEquals(new String[]{"ID", "SITE", "FILE_ID", "LAT", "LONG", "TIME", "CONC_CHL", "KD_490"},
                          header.getAttributeNames());
        assertEquals(true, header.hasLocation());
        assertEquals(true, header.hasTime());

        long t0 = System.currentTimeMillis();
        Iterable<Record> records = recordSource.getRecords();
        int n = 0;
        for (Record record : records) {
             n++;
        }
        assertEquals(11876, n);
        System.out.println("CsvRecordSource read " + n + " records, took " + (System.currentTimeMillis() - t0) + " ms");
    }
}
