package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.GeoPos;

import java.io.*;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

/**
 * A record source that reads from a CSV stream.
 * <p>
 * The expected CSV text format is as follows:<br/>
 * <ul>
 * <li>Values must be separated by a TAB character, records by a NL (newline).</li>
 * <li>Comment lines are ones whose first character is the hash character ('#').
 * Comment lines and empty lines are ignored.</li>
 * <li>The first record must contain the header names. All non-header records must use the same data type in a column.</li>
 * <li>Calvalus expects a geographical point coordinate top be present, the recognised header names are "lat",
 * "latitude", "northing" and "lon", "long", "longitude", "easting" (all case-insensitive).
 * Coordinates must be given as decimal degrees.</li>
 * <li>In order to indicate an optional time(-stamp) value, the header names "time" or "date" (all case-insensitive)
 * are recognised.
 * The usual format for time values is "yyyy-MM-dd HH:mm:ss".</li>
 * <li>Missing numbers (no-data) must be indicated using the string "nan" (case-insensitive).</li>
 * </ul>
 * </p>
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
    private final Class<?>[] attributeTypes;

    public CsvRecordSource(Reader reader, DateFormat dateFormat) throws IOException {
        if (reader instanceof LineNumberReader) {
            this.reader = (LineNumberReader) reader;
        } else {
            this.reader = new LineNumberReader(reader);
        }

        this.dateFormat = dateFormat;

        String[] attributeNames = readTextRecord(-1);
        attributeTypes = new Class<?>[attributeNames.length];

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
            if (recordLength > 0 && strings.size() >= recordLength) {
                break;
            }
            pos1 = pos2 + 1;
        }
        strings.add(line.substring(pos1).trim());
        if (recordLength > 0) {
            return strings.toArray(new String[recordLength]);
        } else {
            return strings.toArray(new String[strings.size()]);
        }
    }

    /**
     * Converts a string array into an array of object which are either a number ({@link Double}), a text ({@link String}),
     * a date/time value ({@link Date}). Empty text is converted to {@code null}.
     *
     * @param textValues The text values to convert.
     * @param types      The types.
     * @param dateFormat The date format to be used.
     * @return The array of converted objects.
     */
    public static Object[] toObjects(String[] textValues, Class<?>[] types, DateFormat dateFormat) {
        final Object[] values = new Object[textValues.length];
        for (int i = 0; i < textValues.length; i++) {
            final String text = textValues[i];
            if (text != null && !text.isEmpty()) {
                final Object value;
                final Class<?> type = types[i];
                if (type != null) {
                    value = parse(text, type, dateFormat);
                } else {
                    value = parse(text, dateFormat);
                    if (value != null) {
                        types[i] = value.getClass();
                    }
                }
                values[i] = value;
            }
        }
        return values;
    }

    private static Object parse(String text, Class<?> type, DateFormat dateFormat) {
        if (type.equals(Double.class)) {
            try {
                return parseDouble(text);
            } catch (NumberFormatException e) {
                return Double.NaN;
            }
        } else if (type.equals(String.class)) {
            return text;
        } else if (type.equals(Date.class)) {
            try {
                return dateFormat.parse(text);
            } catch (ParseException e) {
                return new Date(0L);
            }
        } else {
            throw new IllegalStateException("Unhandled data type: " + type);
        }
    }

    private static Object parse(String text, DateFormat dateFormat) {
        try {
            return parseDouble(text);
        } catch (NumberFormatException e) {
            try {
                return dateFormat.parse(text);
            } catch (ParseException e1) {
                return text;
            }
        }
    }

    private static Double parseDouble(String text) {
        try {
            return Double.valueOf(text);
        } catch (NumberFormatException e) {
            if (text.equalsIgnoreCase("nan")) {
                return Double.NaN;
            } else if (text.equalsIgnoreCase("inf") || text.equalsIgnoreCase("infinity")) {
                return Double.POSITIVE_INFINITY;
            } else if (text.equalsIgnoreCase("-inf") || text.equalsIgnoreCase("-infinity")) {
                return Double.NEGATIVE_INFINITY;
            } else {
                throw e;
            }
        }
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

            final Object[] values = toObjects(textValues, attributeTypes, dateFormat);

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
            return new CsvRecordSource(inputStreamReader, CsvRecordWriter.DEFAULT_DATE_FORMAT);
        }

        @Override
        protected boolean canDecodeContent(String recordSourceUrl) {
            return recordSourceUrl.endsWith(".txt") || recordSourceUrl.endsWith(".csv");
        }

    }

}
