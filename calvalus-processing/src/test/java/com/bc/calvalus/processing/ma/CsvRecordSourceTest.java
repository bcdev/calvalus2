package com.bc.calvalus.processing.ma;

import com.bc.calvalus.commons.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.datamodel.GeoPos;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.StringReader;
import java.text.DateFormat;
import java.util.Date;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class CsvRecordSourceTest {

    private static final DateFormat SHORT_DATE_FORMAT = DateUtils.createDateFormat("dd.MM.yyyy");
    private static final DateFormat LONG_DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd HH:mm:ss");

    @Test
    public void testSimpleCsv() throws Exception {
        final String CSV = ""
                           + "# Test CSV\n"
                           + "ID\tLAT\tLONG\tTIME\tSITE\tCHL\tFLAG\n"
                           + "16\t53.1\t13.6\t03.04.2003\tA\t0.5\t1\n"
                           + "17\t53.3\t13.4\t08.04.2003\tA\t0.9\t0\n"
                           + "18\t53.1\t13.5\t11.04.2003\tA\t0.4\t1\n";


        CsvRecordSource recordSource = new CsvRecordSource(new StringReader(CSV), SHORT_DATE_FORMAT);
        assertHeader(recordSource.getHeader(), true, true, "ID", "LAT", "LONG", "TIME", "SITE", "CHL", "FLAG");

        Iterable<Record> records = recordSource.getRecords();
        assertNotNull(records);

        Iterator<Record> iterator = records.iterator();
        assertNotNull(iterator);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.1F, 13.6F), SHORT_DATE_FORMAT.parse("03.04.2003"), 16.0, 53.1, 13.6, "03.04.2003", "A", 0.5, 1.0);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.3F, 13.4F), SHORT_DATE_FORMAT.parse("08.04.2003"), 17.0, 53.3, 13.4, "08.04.2003", "A", 0.9, 0.0);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.1F, 13.5F), SHORT_DATE_FORMAT.parse("11.04.2003"), 18.0, 53.1, 13.5, "11.04.2003", "A", 0.4, 1.0);

        assertFalse(iterator.hasNext());
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


        CsvRecordSource recordSource = new CsvRecordSource(new StringReader(CSV), SHORT_DATE_FORMAT);
        assertHeader(recordSource.getHeader(), true, true, "ID", "LAT", "LONG", "TIME", "SITE", "CHL", "FLAG");

        Iterable<Record> records = recordSource.getRecords();
        assertNotNull(records);

        Iterator<Record> iterator = records.iterator();
        assertNotNull(iterator);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.1F, 13.6F), SHORT_DATE_FORMAT.parse("03.04.2003"), 16.0, 53.1, 13.6, "03.04.2003", null, 0.5,
                1.0);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.3F, 13.4F), SHORT_DATE_FORMAT.parse("08.04.2003"), 17.0, 53.3, 13.4, "08.04.2003", null, null,
                null);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.1F, 13.5F), SHORT_DATE_FORMAT.parse("11.04.2003"), 18.0, 53.1, 13.5, "11.04.2003", "A", 0.4,
                null);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSimpleCsvWithGivenSeparator() throws Exception {
        final String CSV = ""
                + "# Test CSV\n"
                + "# columnSeparator=,\n"
                + "ID,LAT,LONG,TIME,SITE,CHL,FLAG\n"
                + "16,53.1,13.6,03.04.2003,A,0.5,1\n"
                + "17,53.3,13.4,08.04.2003,A,0.9,0\n"
                + "18,53.1,13.5,11.04.2003,A,0.4,1\n";


        CsvRecordSource recordSource = new CsvRecordSource(new StringReader(CSV), SHORT_DATE_FORMAT);
        assertHeader(recordSource.getHeader(), true, true, "ID", "LAT", "LONG", "TIME", "SITE", "CHL", "FLAG");

        Iterable<Record> records = recordSource.getRecords();
        assertNotNull(records);

        Iterator<Record> iterator = records.iterator();
        assertNotNull(iterator);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.1F, 13.6F), SHORT_DATE_FORMAT.parse("03.04.2003"), 16.0, 53.1, 13.6, "03.04.2003", "A", 0.5, 1.0);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.3F, 13.4F), SHORT_DATE_FORMAT.parse("08.04.2003"), 17.0, 53.3, 13.4, "08.04.2003", "A", 0.9, 0.0);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.1F, 13.5F), SHORT_DATE_FORMAT.parse("11.04.2003"), 18.0, 53.1, 13.5, "11.04.2003", "A", 0.4, 1.0);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSimpleCsvWithGivenLatLonColumns() throws Exception {
        final String CSV = ""
                + "# Test CSV\n"
                + "# latColumn = LA\n"
                + "# lonColumn = LO\n"
                + "ID\tLA\tLO\tTIME\tSITE\tCHL\tFLAG\n"
                + "16\t53.1\t13.6\t03.04.2003\tA\t0.5\t1\n"
                + "17\t53.3\t13.4\t08.04.2003\tA\t0.9\t0\n"
                + "18\t53.1\t13.5\t11.04.2003\tA\t0.4\t1\n";


        CsvRecordSource recordSource = new CsvRecordSource(new StringReader(CSV), SHORT_DATE_FORMAT);
        assertHeader(recordSource.getHeader(), true, true, "ID", "LA", "LO", "TIME", "SITE", "CHL", "FLAG");

        Iterable<Record> records = recordSource.getRecords();
        assertNotNull(records);

        Iterator<Record> iterator = records.iterator();
        assertNotNull(iterator);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.1F, 13.6F), SHORT_DATE_FORMAT.parse("03.04.2003"), 16.0, 53.1, 13.6, "03.04.2003", "A", 0.5, 1.0);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.3F, 13.4F), SHORT_DATE_FORMAT.parse("08.04.2003"), 17.0, 53.3, 13.4, "08.04.2003", "A", 0.9, 0.0);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.1F, 13.5F), SHORT_DATE_FORMAT.parse("11.04.2003"), 18.0, 53.1, 13.5, "11.04.2003", "A", 0.4, 1.0);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSimpleCsvWithGivenTimeColumnButMissingDateFormat() throws Exception {
        final String CSV = ""
                + "# Test CSV\n"
                + "# timeColumn =myTIME\n"
                + "ID\tLAT\tLON\tmyTIME\tSITE\tCHL\tFLAG\n"
                + "16\t53.1\t13.6\t03.04.2003\tA\t0.5\t1\n"
                + "17\t53.3\t13.4\t08.04.2003\tA\t0.9\t0\n"
                + "18\t53.1\t13.5\t11.04.2003\tA\t0.4\t1\n";


        CsvRecordSource recordSource = new CsvRecordSource(new StringReader(CSV), SHORT_DATE_FORMAT);
        assertHeader(recordSource.getHeader(), true, true, "ID", "LAT", "LON", "myTIME", "SITE", "CHL", "FLAG");

        Iterable<Record> records = recordSource.getRecords();
        assertNotNull(records);

        Iterator<Record> iterator = records.iterator();
        assertNotNull(iterator);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.1F, 13.6F), SHORT_DATE_FORMAT.parse("03.04.2003"), 16.0, 53.1, 13.6, "03.04.2003", "A", 0.5, 1.0);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.3F, 13.4F), SHORT_DATE_FORMAT.parse("08.04.2003"), 17.0, 53.3, 13.4, "08.04.2003", "A", 0.9, 0.0);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.1F, 13.5F), SHORT_DATE_FORMAT.parse("11.04.2003"), 18.0, 53.1, 13.5, "11.04.2003", "A", 0.4, 1.0);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSimpleCsvWithGivenCombinedTimeColumn() throws Exception {
        final String CSV = ""
                + "# Test CSV\n"
                + "# timeColumns=day,month,year\n"
                + "# dateFormat=dd,MM,yyyy\n"
                + "ID\tLAT\tLON\tday\tyear\tmonth\tSITE\tCHL\tFLAG\n"
                + "16\t53.1\t13.6\t03\t2003\t04\tA\t0.5\t1\n"
                + "17\t53.3\t13.4\t08\t2003\t04\tA\t0.9\t0\n"
                + "18\t53.1\t13.5\t11\t2003\t04\tA\t0.4\t1\n";


        CsvRecordSource recordSource = new CsvRecordSource(new StringReader(CSV), SHORT_DATE_FORMAT);
        assertHeader(recordSource.getHeader(), true, true, "ID", "LAT", "LON", "day", "year", "month", "SITE", "CHL", "FLAG");

        Iterable<Record> records = recordSource.getRecords();
        assertNotNull(records);

        Iterator<Record> iterator = records.iterator();
        assertNotNull(iterator);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.1F, 13.6F), SHORT_DATE_FORMAT.parse("03.04.2003"), 16.0, 53.1, 13.6, 3.0, 2003.0, 4.0, "A", 0.5,
                1.0);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.3F, 13.4F), SHORT_DATE_FORMAT.parse("08.04.2003"), 17.0, 53.3, 13.4, 8.0, 2003.0, 4.0, "A", 0.9,
                0.0);

        assertTrue(iterator.hasNext());
        assertRecord(iterator.next(), new GeoPos(53.1F, 13.5F), SHORT_DATE_FORMAT.parse("11.04.2003"), 18.0, 53.1, 13.5, 11.0, 2003.0, 4.0, "A", 0.4,
                1.0);

        assertFalse(iterator.hasNext());
    }


    @Test
    public void testSimpleCsvWithGivenCombinedTimeColumnsButMissingDateFormat() throws Exception {
        final String CSV = ""
                + "# Test CSV\n"
                + "# timeColumns = day,month,year\n"
                + "ID\tLAT\tLON\tmyTIME\tSITE\tCHL\tFLAG\n"
                + "16\t53.1\t13.6\t03.04.2003\tA\t0.5\t1\n"
                + "17\t53.3\t13.4\t08.04.2003\tA\t0.9\t0\n"
                + "18\t53.1\t13.5\t11.04.2003\tA\t0.4\t1\n";

        try {
            CsvRecordSource recordSource = new CsvRecordSource(new StringReader(CSV), SHORT_DATE_FORMAT);
        } catch (Exception e) {
            assertEquals("header parameter 'timeColumns' requires a user supplied 'dateFormat'.", e.getMessage());

        }
    }

    @Test
    public void testRealLifeCsv() throws Exception {
        InputStreamReader reader = new InputStreamReader(getClass().getResourceAsStream("cc-matchup-test-insitu.csv"));
        CsvRecordSource recordSource = new CsvRecordSource(reader, CsvRecordWriter.DEFAULT_DATE_FORMAT);

        assertHeader(recordSource.getHeader(), true, true, "ID", "SITE", "FILE_ID", "LAT", "LONG", "TIME", "CONC_CHL", "KD_490");

        long t0 = System.currentTimeMillis();
        Iterable<Record> records = recordSource.getRecords();
        int n = 0;
        for (Record record : records) {
            Object[] dataAttributeValues = record.getAttributeValues();
            assertEquals(8, dataAttributeValues.length);
            assertType(n, Double.class, dataAttributeValues[0]);
            assertType(n, Double.class, dataAttributeValues[1]);
            assertType(n, Double.class, dataAttributeValues[2]);
            assertType(n, Double.class, dataAttributeValues[3]);
            assertType(n, Double.class, dataAttributeValues[4]);
            assertType(n, String.class, dataAttributeValues[5]);
            assertType(n, Double.class, dataAttributeValues[6]);
            assertType(n, Double.class, dataAttributeValues[7]);
            n++;
        }
        assertEquals(11876, n);
        System.out.println("CsvRecordSource read " + n + " records, took " + (System.currentTimeMillis() - t0) + " ms");
    }

    @Test
    public void testBadCsvWithMalformedTime() throws Exception {
        final String CSV = ""
                + "\n"
                + "# Test CSV\n"
                + "\n"
                + "ID\tLAT\tLONG\tTIME\tSITE\tCHL\tFLAG\n"
                + "16\t53.1\t13.6\t03.04.2003\t\t0.5\t1\n"
                + "17\t53.3\t13.4\t08.04.2003\t\t\t\n"
                + "18\t53.1\t13.5\t11.04.2003\tA\t0.4\t\n";


        try {
            CsvRecordSource recordSource = new CsvRecordSource(new StringReader(CSV), LONG_DATE_FORMAT);

            assertHeader(recordSource.getHeader(), true, true, "ID", "LAT", "LONG", "TIME", "SITE", "CHL", "FLAG");

            Iterable<Record> records = recordSource.getRecords();
            for (Record record : records) {
            }
            fail("error not detected");
        } catch (Exception e) {
            assertEquals("time value '03.04.2003' in line 5 column 3 of point data file not well-formed (pattern yyyy-MM-dd HH:mm:ss expected)",
                    e.getMessage());
        }
    }

    @Test
    public void testBadCsvWithNoTime() throws Exception {
        final String CSV = ""
                + "\n"
                + "# Test CSV\n"
                + "\n"
                + "ID\tLAT\tLONG\tNoTIME\tSITE\tCHL\tFLAG\n"
                + "16\t53.1\t13.6\t03.04.2003\t\t0.5\t1\n"
                + "17\t53.3\t13.4\t08.04.2003\t\t\t\n"
                + "18\t53.1\t13.5\t11.04.2003\tA\t0.4\t\n";

        CsvRecordSource recordSource = new CsvRecordSource(new StringReader(CSV), SHORT_DATE_FORMAT);

        assertHeader(recordSource.getHeader(), true, false, "ID", "LAT", "LONG", "NoTIME", "SITE", "CHL", "FLAG");

        Iterable<Record> records;
        records = recordSource.getRecords();
        for (Record record : records) {
            assertNull("time value", record.getTime());
        }
    }

    @Test
    public void testBadCsvWithMalformedLat() throws Exception {
        final String CSV = ""
                + "\n"
                + "# Test CSV\n"
                + "\n"
                + "ID\tLAT\tLONG\tTIME\tSITE\tCHL\tFLAG\n"
                + "16\t53.1\t13.6\t2003-04-03 00:00:00\t\t0.5\t1\n"
                + "17\t\t13.4\t2003-04-08 00:00:00\t\t\t\n"
                + "18\t53.1\t13.5\t2003-04-11 00:00:00\tA\t0.4\t\n";

        try {
            CsvRecordSource recordSource = new CsvRecordSource(new StringReader(CSV), LONG_DATE_FORMAT);

            assertHeader(recordSource.getHeader(), true, true, "ID", "LAT", "LONG", "TIME", "SITE", "CHL", "FLAG");

            Iterable<Record> records;
            records = recordSource.getRecords();
            for (Record record : records) {
                record.getLocation().getLat();
            }
            fail("error not detected");
        } catch (Exception e) {
            assertEquals("lat and lon value '' and '13.4' in line 6 column 1 and 2 of point data file not well-formed numbers", e.getMessage());
        }
    }

    @Test
    public void testBadCsvWithNoLat() throws Exception {
        final String CSV = ""
                + "\n"
                + "# Test CSV\n"
                + "\n"
                + "ID\tNoLAT\tLONG\tTIME\tSITE\tCHL\tFLAG\n"
                + "16\t53.1\t13.6\t2003-04-03 00:00:00\t\t0.5\t1\n"
                + "17\t53.3\t13.4\t2003-04-08 00:00:00\t\t\t\n"
                + "18\t53.1\t13.5\t2003-04-11 00:00:00\tA\t0.4\t\n";

        try {
            CsvRecordSource recordSource = new CsvRecordSource(new StringReader(CSV), SHORT_DATE_FORMAT);
            Iterable<Record> records;
            records = recordSource.getRecords();
            for (Record record : records) {
                record.getLocation();
            }
            fail("error not detected");
        } catch (Exception e) {
            assertEquals(
                    "missing lat and lon columns in header of point data file (one of lat, latitude, northing and one of lon, long, longitude, easting expected)",
                    e.getMessage());
        }
    }

    @Test
    public void testRealLifeCsvWithMalformedTime() throws Exception {
        try {
            final String url = this.getClass().getResource("/point-data-bad-time-format.txt").toExternalForm();
            MAConfig maConfig = new MAConfig();
            maConfig.setRecordSourceUrl(url);
            final RecordSource recordSource = maConfig.createRecordSource(new Configuration());
            Iterable<Record> records = recordSource.getRecords();
            for (Record record : records) {
                record.getTime();
            }
            fail("error not detected");
        } catch (Exception e) {
            assertEquals("time value '08.05.2006' in line 3 column 3 of point data file not well-formed (pattern yyyy-MM-dd HH:mm:ss expected)",
                    e.getMessage());
        }
    }

    @Test
    public void testBadFilenameExtension() throws Exception {
        try {
            final String url = this.getClass().getResource("/point-data-bad-filename-extension.doc").toExternalForm();
            MAConfig maConfig = new MAConfig();
            maConfig.setRecordSourceUrl(url);
            final RecordSource recordSource = maConfig.createRecordSource(new Configuration());
            fail("error not detected");
        } catch (Exception e) {
            String message = e.getMessage();
            assertThat(message, containsString("no record source reader found for filename extension of file:"));
            assertThat(message, containsString("point-data-bad-filename-extension.doc point data file (one of .placemark, .txt, .csv expected)"));
        }
    }

    private static void assertHeader(Header header, boolean expectedHasLocation, boolean expectedHasTime, String... expectedAttributeNames) {
        assertNotNull(header);

        assertEquals(expectedHasLocation, header.hasLocation());
        assertEquals(expectedHasTime, header.hasTime());

        String[] attributeNames = header.getAttributeNames();
        assertNotNull(attributeNames);
        assertArrayEquals(expectedAttributeNames, attributeNames);
    }

    private static void assertRecord(Record record, GeoPos expectedLocation, Date expectedTime, Object... expectedAttributes) {
        assertNotNull(record);

        assertEquals(expectedLocation, record.getLocation());
        assertEquals(expectedTime, record.getTime());

        assertArrayEquals(expectedAttributes, record.getAttributeValues());
    }

    private static void assertType(int n, Class<?> expectedType, Object attributeValue) {
        if (attributeValue != null) {
            assertEquals(String.format("Record #%d: value=%s ", (n + 1), attributeValue), expectedType, attributeValue.getClass());
        }
    }
}
