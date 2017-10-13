package com.bc.calvalus.processing.ma;

import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.datamodel.GeoPos;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.DateFormat;
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
 * <li>Calvalus expects a geographical point coordinate to be present, the recognised header names are "lat",
 * "latitude", "northing" and "lon", "long", "longitude", "easting" (all case-insensitive).
 * Coordinates must be given as decimal degrees.</li>
 * <li>In order to indicate an optional time(-stamp) value, the header names "time" or "date" (all case-insensitive)
 * are recognised.
 * The usual format for time values is "yyyy-MM-dd HH:mm:ss".</li>
 * <li>Missing numbers (no-data) must be indicated using the string "nan" (case-insensitive).</li>
 * </ul>
 * </p>
 * <p>
 * If the format of the CSV text differs from the given default structure.
 * A number of parameters can be set in comment lines (lines starting with '#') in the format "key=value":
 * <ul>
 *     <li>'latColumn': the name of the column containing the latitude values.</li>
 *     <li>'lonColumn': the name of the column containing the longitude values.</li>
 *     <li>'timeColumn': the name of the column containing the date values.</li>
 *     <li>'timeColumns': a comma separated list of column names containing the date/time information.
 *     The values of the columns are concatenated separated by the pipe character ('|').
 *     For parsing this combined value a 'dateFormat' has to be given as well.</li>
 *     <li>'dateFormat': as {@link DateFormat} for interpreting the date/time information.</li>
 *     <li>'columnSeparator': the character that separates different columns on a line.</li>
 * </ul>
 * </p>
 *
 * @author Norman
 * @author MarcoZ
 */
public class CsvRecordSource implements RecordSource {

    private final Header header;
    private final int recordLength;
    private final int latIndex;
    private final int lonIndex;
    private final Class<?>[] attributeTypes;
    private final CsvLineReader csvLineReader;

    public CsvRecordSource(Reader reader, DateFormat dateFormat) throws IOException {
        csvLineReader = new CsvLineReader(reader, dateFormat);

        String[] attributeNames = csvLineReader.getAttributeNames();
        attributeTypes = new Class<?>[attributeNames.length];

        latIndex = csvLineReader.getLatIndex();
        lonIndex = csvLineReader.getLonIndex();
        header = new DefaultHeader(latIndex >= 0 && lonIndex >= 0, csvLineReader.hasTime(), attributeNames);
        recordLength = attributeNames.length;
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

    @Override
    public String getTimeAndLocationColumnDescription() {
        String timeColumnNames = csvLineReader.getTimeColumnNames();
        String latLonMsg = "columns for lat=\"" + getHeader().getAttributeName(latIndex)
                + "\" lon=\"" + getHeader().getAttributeName(lonIndex);
        if (timeColumnNames != null) {
            latLonMsg = latLonMsg.concat("\" time=\"" + timeColumnNames + "\"");
        }
        return latLonMsg;
    }

    /**
     * Converts a string array into an array of object which are either a number ({@link Double}), a text ({@link String}).
     * Empty text is converted to {@code null}.
     *
     * @param textValues The text values to convert.
     * @param types      The types.
     * @return The array of converted objects.
     */
    public static Object[] toObjects(String[] textValues, Class<?>[] types) {
        final Object[] values = new Object[textValues.length];
        for (int i = 0; i < textValues.length; i++) {
            final String text = textValues[i];
            if (text != null && !text.isEmpty()) {
                final Object value;
                final Class<?> type = types[i];
                if (type != null) {
                    value = parse(text, type);
                } else {
                    value = parse(text);
                    if (value != null) {
                        types[i] = value.getClass();
                    }
                }
                values[i] = value;
            }
        }
        return values;
    }

    private static Object parse(String text, Class<?> type) {
        if (type.equals(Double.class)) {
            try {
                return parseDouble(text);
            } catch (NumberFormatException e) {
                return Double.NaN;
            }
        } else if (type.equals(String.class)) {
            return text;
        } else {
            throw new IllegalStateException("Unhandled data type: " + type);
        }
    }

    private static Object parse(String text) {
        try {
            return parseDouble(text);
        } catch (NumberFormatException e) {
            return text;
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
                textValues = csvLineReader.readTextRecord(recordLength);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (textValues == null) {
                return null;
            }

            if (getHeader().getAttributeNames().length != textValues.length) {
                throw new IllegalArgumentException("different number of columns " + textValues.length
                                                   + " instead of " + getHeader().getAttributeNames().length
                                                   + " in line " + csvLineReader.getLineNumber() +
                                                   " of point data file");
            }

            final Object[] values = toObjects(textValues, attributeTypes);

            final GeoPos location;
            if (! header.hasLocation()) {
                String msg = "missing lat and lon columns in header of point data file (one of " +
                        csvLineReader.getLatNames() + " and one of " + csvLineReader.getLonNames() + " expected)";
                throw new IllegalArgumentException(msg);
            } else if (values[latIndex] instanceof Number && values[lonIndex] instanceof Number) {
                location = new GeoPos(((Number) values[latIndex]).floatValue(),
                                      ((Number) values[lonIndex]).floatValue());
                if (location.getLat() < -90.0f || location.getLat() > 90.0f || location.getLon() < -180.0f || location.getLon() > 360.0f) {
                    throw new IllegalArgumentException("lat and lon value '" + textValues[latIndex]
                                           + "' and '" + textValues[lonIndex]
                                           + "' in line " + csvLineReader.getLineNumber() + " column " + latIndex + " and " + lonIndex
                                           + " of point data file out of range [-90..90] or [-180..360]");
                }
            } else {
                throw new IllegalArgumentException("lat and lon value '" + textValues[latIndex]
                       + "' and '" + textValues[lonIndex]
                       + "' in line " + csvLineReader.getLineNumber() + " column " + latIndex + " and " + lonIndex
                       + " of point data file not well-formed numbers");
            }

            final Date time;
            if (! header.hasTime()) {
                time = null;
            } else {
                time = csvLineReader.extractTime(values, csvLineReader.getLineNumber());
            }

            return new DefaultRecord(csvLineReader.getLineNumber(), location, time, values);
        }

    }

    public static class Spi extends RecordSourceSpi {

        @Override
        public RecordSource createRecordSource(String url, Configuration conf) throws Exception {
            InputStream inputStream = HadoopProcessingService.openUrlAsStream(url, conf);
            Reader reader = new BufferedReader(new InputStreamReader(inputStream));
            return new CsvRecordSource(reader, CsvRecordWriter.DEFAULT_DATE_FORMAT);
        }

        @Override
        public String[] getAcceptedExtensions() {
            return new String[] { ".txt", ".csv" };
        }
    }

}
