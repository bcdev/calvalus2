package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.ProductData;

import java.io.IOException;
import java.io.Writer;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;

import static com.bc.calvalus.processing.ma.PixelExtractor.*;

/**
 * Outputs records to 2 CSV files: records-all.txt and records-agg.txt
 *
 * @author Norman
 */
public class CsvRecordWriter implements RecordProcessor {
    public static final DateFormat DEFAULT_DATE_FORMAT = ProductData.UTC.createDateFormat("yyyy-MM-dd hh:mm:ss");
    public static final char DEFAULT_COLUMN_SEPARATOR_CHAR = '\t';
    static final String SUFFIX_MEAN = "_mean";
    static final String SUFFIX_SIGMA = "_sigma";
    static final String SUFFIX_N = "_n";

    private final Writer recordsAllWriter;
    private final Writer recordsAggWriter;

    private char separatorChar;
    private DateFormat dateFormat;

    public CsvRecordWriter(Writer recordsAllWriter, Writer recordsAggWriter) {
        this.recordsAllWriter = recordsAllWriter;
        this.recordsAggWriter = recordsAggWriter;
        separatorChar =  DEFAULT_COLUMN_SEPARATOR_CHAR;
        dateFormat =  DEFAULT_DATE_FORMAT;
    }

    public DateFormat getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(DateFormat dateFormat) {
        this.dateFormat = dateFormat;
    }

    public char getSeparatorChar() {
        return separatorChar;
    }

    public void setSeparatorChar(char separatorChar) {
        this.separatorChar = separatorChar;
    }

    public static String[] transformHeader(String[] attributeNames, boolean makeStatColumns) {
        ArrayList<String> strings = new ArrayList<String>(attributeNames.length * (makeStatColumns ? 3 : 1));
        for (String attributeName : attributeNames) {
            if (attributeName.startsWith(AGGREGATION_PREFIX)) {
                String name = attributeName.substring(AGGREGATION_PREFIX.length());
                if (makeStatColumns) {
                    strings.add(name + SUFFIX_MEAN);
                    strings.add(name + SUFFIX_SIGMA);
                    strings.add(name + SUFFIX_N);
                } else {
                    strings.add(name);
                }
            } else {
                strings.add(attributeName);
            }
        }
        return strings.toArray(new String[strings.size()]);
    }

    @Override
    public void processHeaderRecord(Object[] headerValues) throws IOException {

        recordsAllWriter.write(headerRecordToString(headerValues, false, separatorChar));
        recordsAllWriter.write("\n");

        recordsAggWriter.write(headerRecordToString(headerValues, true, separatorChar));
        recordsAggWriter.write("\n");
    }

    @Override
    public void processDataRecord(int recordIndex, Object[] recordValues) throws IOException {
        // todo - write a record for each of the common-length 'data' arrays in of all AggregatedNumbers
        recordsAllWriter.write(dataRecordToString(recordValues, false, separatorChar, dateFormat));
        recordsAllWriter.write("\n");

        recordsAggWriter.write(dataRecordToString(recordValues, true, separatorChar, dateFormat));
        recordsAggWriter.write("\n");
    }

    @Override
    public void finalizeRecordProcessing(int numRecords) throws IOException {
        recordsAllWriter.close();
        recordsAggWriter.close();
    }

    public static String recordToString(Object[] values) {
        return dataRecordToString(values, false, DEFAULT_COLUMN_SEPARATOR_CHAR, DEFAULT_DATE_FORMAT);
    }

    public static String headerRecordToString(Object[] values, boolean statColumns, char separatorChar) {
        if (values == null) {
            return "";
        }
        final StringBuilder sb = new StringBuilder(values.length * 10);
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(separatorChar);
            }
            final Object value = values[i];
            if (value != null) {
                String name = value.toString();
                if (name.startsWith(AGGREGATION_PREFIX)) {
                    name = name.substring(AGGREGATION_PREFIX.length());
                    if (statColumns) {
                        sb.append(name + SUFFIX_MEAN);
                        sb.append(separatorChar);
                        sb.append(name + SUFFIX_SIGMA);
                        sb.append(separatorChar);
                        sb.append(name + SUFFIX_N);
                    } else {
                        sb.append(name);
                    }
                } else {
                    sb.append(name);
                }
            }
        }
        return sb.toString();
    }

    public static String dataRecordToString(Object[] values, boolean statColumns, char separatorChar, DateFormat dateFormat) {
        if (values == null) {
            return "";
        }
        final StringBuilder sb = new StringBuilder(values.length * 10);
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(separatorChar);
            }
            final Object value = values[i];
            if (value != null) {
                if (value instanceof AggregatedNumber) {
                    if (statColumns) {
                        AggregatedNumber aggregatedNumber = (AggregatedNumber) value;
                        sb.append(aggregatedNumber.mean);
                        sb.append(separatorChar);
                        sb.append(aggregatedNumber.sigma);
                        sb.append(separatorChar);
                        sb.append(aggregatedNumber.n);
                    } else {
                        sb.append(value.toString());
                    }
                } else if (value instanceof Number) {
                    // most values should be numbers, so do instanceof before Date
                    sb.append(value.toString());
                } else if (value instanceof Date) {
                    if (dateFormat == null) {
                        dateFormat = DEFAULT_DATE_FORMAT;
                    }
                    sb.append(dateFormat.format((Date) value));
                } else {
                    sb.append(value.toString());
                }
            }
        }

        return sb.toString();
    }
}
