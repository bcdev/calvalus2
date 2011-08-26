package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.util.io.CsvReader;

import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
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
    private final CsvReader csvReader;
    private final Header header;
    private final DateFormat dateFormat;
    private final int latIndex;
    private final int lonIndex;
    private final int timeIndex;

    public CsvRecordSource(Reader reader, DateFormat dateFormat) throws IOException {
        this.dateFormat = dateFormat;
        csvReader = new CsvReader(reader, new char[]{'\t'}, true, "#");
        String[] attributeNames = csvReader.readRecord();

        latIndex = TextUtils.indexOf(attributeNames, LAT_NAMES);
        lonIndex = TextUtils.indexOf(attributeNames, LON_NAMES);
        timeIndex = TextUtils.indexOf(attributeNames, TIME_NAMES);

        header = new DefaultHeader(latIndex >= 0 && lonIndex >= 0, timeIndex >= 0, attributeNames);
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


    private class CsvRecordIterator extends RecordIterator {
        @Override
        protected Record getNextRecord() {

            final String[] textValues;
            try {
                textValues = csvReader.readRecord();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            final Object[] values = TextUtils.convert(textValues, dateFormat);

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
}
