package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.ProductData;

import java.io.IOException;
import java.io.Writer;
import java.text.DateFormat;
import java.util.Date;

import static com.bc.calvalus.processing.ma.PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX;

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
        separatorChar = DEFAULT_COLUMN_SEPARATOR_CHAR;
        dateFormat = DEFAULT_DATE_FORMAT;
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

    @Override
    public void processHeaderRecord(Object[] headerValues) throws IOException {

        recordsAllWriter.write(headerRecordToString(headerValues, false));
        recordsAllWriter.write("\n");

        recordsAggWriter.write(headerRecordToString(headerValues, true));
        recordsAggWriter.write("\n");
    }

    @Override
    public void processDataRecord(int recordIndex, Object[] recordValues) throws IOException {
        int length = getCommonDataArrayLength(recordValues);
        if (length > 0) {
            for (int i = 0; i < length; i++) {
                recordsAllWriter.write(dataRecordToString(recordValues, false, i));
                recordsAllWriter.write("\n");
            }
        } else {
            recordsAllWriter.write(dataRecordToString(recordValues, false, -1));
            recordsAllWriter.write("\n");
        }

        recordsAggWriter.write(dataRecordToString(recordValues, true, -1));
        recordsAggWriter.write("\n");
    }

    @Override
    public void finalizeRecordProcessing(int numRecords) throws IOException {
        recordsAllWriter.close();
        recordsAggWriter.close();
    }

    public static String toString(Object[] values) {
        return dataRecordToString(values, false, -1, DEFAULT_COLUMN_SEPARATOR_CHAR, DEFAULT_DATE_FORMAT);
    }

    public String headerRecordToString(Object[] values, boolean statColumns) {
        return headerRecordToString(values, statColumns, separatorChar);
    }

    public String dataRecordToString(Object[] values, boolean statColumns, int dataIndex) {
        return dataRecordToString(values, statColumns, dataIndex, separatorChar, dateFormat);
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
                if (name.startsWith(ATTRIB_NAME_AGGREG_PREFIX)) {
                    name = name.substring(ATTRIB_NAME_AGGREG_PREFIX.length());
                    if (statColumns) {
                        sb.append(name).append(SUFFIX_MEAN).append(separatorChar);
                        sb.append(name).append(SUFFIX_SIGMA).append(separatorChar);
                        sb.append(name).append(SUFFIX_N);
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

    public static String dataRecordToString(Object[] values, boolean statColumns, int dataIndex, char separatorChar, DateFormat dateFormat) {
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
                    AggregatedNumber aggregatedNumber = (AggregatedNumber) value;
                    if (statColumns) {
                        sb.append(aggregatedNumber.mean).append(separatorChar);
                        sb.append(aggregatedNumber.sigma).append(separatorChar);
                        sb.append(aggregatedNumber.n);
                    } else if (dataIndex >= 0) {
                        sb.append(String.valueOf(aggregatedNumber.data[dataIndex]));
                    } else {
                        sb.append(aggregatedNumber.toString());
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


    static int getCommonDataArrayLength(Object[] attributeValues) {
        int commonLength = -1;
        for (Object attributeValue : attributeValues) {
            if (attributeValue instanceof AggregatedNumber) {
                AggregatedNumber value = (AggregatedNumber) attributeValue;
                int length = value.data != null ? value.data.length : 0;
                if (commonLength >= 0 && length != commonLength) {
                    throw new IllegalArgumentException(String.format("Record with varying array lengths detected. Expected %d, found %d",
                                                                     commonLength, length));
                }
                commonLength = length;
            }
        }
        return commonLength;
    }

}
