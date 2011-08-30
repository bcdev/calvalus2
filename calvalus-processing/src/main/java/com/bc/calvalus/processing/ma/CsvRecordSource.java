package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.GeoPos;

import java.io.*;
import java.net.URL;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

/**
 * A record source that reads from a CSV stream. Values must be separated by a TAB character, records by a NL (newline).
 * The first records must contain header names. All non-header records must use the same data type in a column.
 *
 * @author Norman
 */
public class CsvRecordSource implements RecordSource {

    private static final String[] LAT_NAMES = new String[]{"lat", "latitude", "northing"};
    private static final String[] LON_NAMES = new String[]{"lon", "long", "longitude", "easting"};
    private static final String[] TIME_NAMES = new String[]{"time", "date"};
    private final LineNumberReader reader;
    private final Header header;
    private final int recordLength;
    private final DateFormat dateFormat;
    private final int latIndex;
    private final int lonIndex;
    private final int timeIndex;

    public CsvRecordSource(Reader reader, DateFormat dateFormat) throws IOException {
        if (reader instanceof LineNumberReader) {
            this.reader = (LineNumberReader) reader;
        } else {
            this.reader = new LineNumberReader(reader);
        }

        this.dateFormat = dateFormat;

        String[] attributeNames = readTextRecord(-1);

        latIndex = TextUtils.indexOf(attributeNames, LAT_NAMES);
        lonIndex = TextUtils.indexOf(attributeNames, LON_NAMES);
        timeIndex = TextUtils.indexOf(attributeNames, TIME_NAMES);

        header = new DefaultHeader(latIndex >= 0 && lonIndex >= 0, timeIndex >= 0, attributeNames);
        recordLength = attributeNames.length;
    }

    private String[] readTextRecord(int recordLength) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            String trimLine = line.trim();
            if (!trimLine.startsWith("#") && !trimLine.isEmpty()) {
                return splitRecordLine(line, recordLength);
            }
        }
        return null;
    }

    @Override
    public Header getHeader() {
        return header;
    }

    @Override
    public Iterable<Record> getRecords() throws Exception {
        return new Iterable<Record>() {
            @Override
            public Iterator<Record> iterator() {
                return new CsvRecordIterator();
            }
        };
    }

    private static String[] splitRecordLine(String line, int recordLength) {
        int pos2;
        int pos1 = 0;
        ArrayList<String> strings = new ArrayList<String>(256);
        while ((pos2 = line.indexOf('\t', pos1)) >= 0) {
            strings.add(line.substring(pos1, pos2).trim());
            if (recordLength > 0 && strings.size() == recordLength) {
                break;
            }
            pos1 = pos2 + 1;
        }
        strings.add(line.substring(pos1).trim());
        return strings.toArray(new String[strings.size()]);
    }

    private class CsvRecordIterator extends RecordIterator {
        @Override
        protected Record getNextRecord() {

            final String[] textValues;
            try {
                textValues = readTextRecord(recordLength);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (textValues == null) {
                return null;
            }

            if (getHeader().getAttributeNames().length != textValues.length) {
                System.out.println("to less values " + Arrays.toString(textValues));
            }

            final Object[] values = TextUtils.toObjects(textValues, dateFormat);

            final GeoPos location;
            if (header.hasLocation() && values[latIndex] instanceof Number && values[lonIndex] instanceof Number) {
                location = new GeoPos(((Number) values[latIndex]).floatValue(),
                                      ((Number) values[lonIndex]).floatValue());
            } else {
                location = null;
            }

            final Date time;
            if (header.hasTime() && values[timeIndex] instanceof Date) {
                time = values[timeIndex] instanceof Date ? (Date) values[timeIndex] : null;
            } else {
                time = null;
            }

            return new DefaultRecord(location, time, values);
        }

    }

    public static class Spi extends RecordSourceSpi {

        @Override
        public RecordSource createRecordSource(String url) throws Exception {
            InputStream inputStream = new URL(url).openStream();
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            return new CsvRecordSource(inputStreamReader, TextUtils.DEFAULT_DATE_FORMAT);
        }
    }

}
