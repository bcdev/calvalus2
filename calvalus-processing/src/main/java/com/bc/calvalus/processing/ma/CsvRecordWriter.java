package com.bc.calvalus.processing.ma;

import com.bc.calvalus.commons.DateUtils;

import java.io.IOException;
import java.io.Writer;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;

import static com.bc.calvalus.processing.ma.PixelExtractor.*;

/**
 * Outputs records to 2 CSV files: records-all.txt and records-agg.txt
 *
 * @author Norman
 */
public class CsvRecordWriter implements RecordProcessor {

    public static final DateFormat DEFAULT_DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final char DEFAULT_COLUMN_SEPARATOR_CHAR = '\t';
    static final String SUFFIX_MEAN = "_mean";
    static final String SUFFIX_SIGMA = "_sigma";
    static final String SUFFIX_N = "_n";
    private static final String CALVALUS_ID = "CALVALUS_ID";

    private Writer recordsAllWriter;
    private Writer recordsAggWriter;
    private Writer labeledAllRecordsWriter;
    private Writer labeledAggRecordsWriter;

    private final char separatorChar;
    private final DateFormat dateFormat;
    private int exclusionIndex;

    public CsvRecordWriter(Writer recordsAllWriter, Writer recordsAggWriter) {
        this(recordsAllWriter, recordsAggWriter, null, null);
    }

    public CsvRecordWriter(Writer recordsAllWriter, Writer recordsAggWriter, Writer labeledAllRecordsWriter, Writer labeledAggRecordsWriter) {
        this.recordsAllWriter = recordsAllWriter;
        this.recordsAggWriter = recordsAggWriter;
        this.labeledAllRecordsWriter = labeledAllRecordsWriter;
        this.labeledAggRecordsWriter = labeledAggRecordsWriter;
        separatorChar = DEFAULT_COLUMN_SEPARATOR_CHAR;
        dateFormat = DEFAULT_DATE_FORMAT;
    }

    @Override
    public void processHeaderRecord(Object[] attributeNames, Object[] annotationNames) throws IOException {

        String annotationHeader = attributeNamesToString(annotationNames, false);
        String allHeader = attributeNamesToString(attributeNames, false);
        String aggHeader = attributeNamesToString(attributeNames, true);

        recordsAllWriter.write(CALVALUS_ID);
        recordsAllWriter.write(separatorChar);
        recordsAllWriter.write(allHeader);
        recordsAllWriter.write("\n");

        if (labeledAllRecordsWriter != null) {
            labeledAllRecordsWriter.write(CALVALUS_ID);
            labeledAllRecordsWriter.write(separatorChar);
            labeledAllRecordsWriter.write(allHeader);
            labeledAllRecordsWriter.write(separatorChar);
            labeledAllRecordsWriter.write(annotationHeader);
            labeledAllRecordsWriter.write("\n");
        }

        recordsAggWriter.write(CALVALUS_ID);
        recordsAggWriter.write(separatorChar);
        recordsAggWriter.write(aggHeader);
        recordsAggWriter.write("\n");

        if (labeledAggRecordsWriter != null) {
            labeledAggRecordsWriter.write(CALVALUS_ID);
            labeledAggRecordsWriter.write(separatorChar);
            labeledAggRecordsWriter.write(aggHeader);
            labeledAggRecordsWriter.write(separatorChar);
            labeledAggRecordsWriter.write(annotationHeader);
            labeledAggRecordsWriter.write("\n");
        }

        exclusionIndex = Arrays.asList(annotationNames).indexOf(DefaultHeader.ANNOTATION_EXCLUSION_REASON);
    }

    @Override
    public void processDataRecord(String key, Object[] attributeValues, Object[] annotationValues) throws IOException {
        int length = getCommonDataArrayLength(attributeValues);
        String reason = "";
        if (exclusionIndex >= 0) {
            reason = (String) annotationValues[exclusionIndex];
        }
        String annotationLine = dataRecordToString(annotationValues, false, -1);
        if (length > 0) {
            for (int i = 0; i < length; i++) {
                String line =  dataRecordToString(attributeValues, false, i);
                if (reason.isEmpty()) {
                    recordsAllWriter.write(key + separatorChar + line + "\n");
                }
                if (labeledAllRecordsWriter != null) {
                    labeledAllRecordsWriter.write(key + separatorChar + line + separatorChar + annotationLine + "\n");
                }
            }
        } else {
            String line = dataRecordToString(attributeValues, false, -1);
            if (reason.isEmpty()) {
                recordsAllWriter.write(key + separatorChar + line + "\n");
            }
            if (labeledAllRecordsWriter != null) {
                labeledAllRecordsWriter.write(key + separatorChar + line + separatorChar + annotationLine + "\n");
            }
        }

        String line = dataRecordToString(attributeValues, true, -1);
        if (reason.isEmpty()) {
            recordsAggWriter.write(key + separatorChar + line + "\n");
        }
        if (labeledAggRecordsWriter != null) {
            labeledAggRecordsWriter.write(key + separatorChar + line + separatorChar + annotationLine + "\n");
        }
    }

    @Override
    public void finalizeRecordProcessing() throws IOException {
        if (recordsAllWriter != null) {
            recordsAllWriter.close();
            recordsAllWriter = null;
        }
        if (recordsAggWriter != null) {
            recordsAggWriter.close();
            recordsAggWriter = null;
        }
        if (labeledAllRecordsWriter != null) {
            labeledAllRecordsWriter.close();
            labeledAllRecordsWriter = null;
        }
        if (labeledAggRecordsWriter != null) {
            labeledAggRecordsWriter.close();
            labeledAggRecordsWriter = null;
        }
    }

    public static String toString(Object[] values) {
        return dataRecordToString(values, false, -1, DEFAULT_COLUMN_SEPARATOR_CHAR, DEFAULT_DATE_FORMAT);
    }

    private String attributeNamesToString(Object[] values, boolean statColumns) {
        return headerRecordToString(values, statColumns, separatorChar);
    }

    private String dataRecordToString(Object[] values, boolean statColumns, int aggregatedDataIndex) {
        return dataRecordToString(values, statColumns, aggregatedDataIndex, separatorChar, dateFormat);
    }

    private static String headerRecordToString(Object[] values, boolean statColumns, char separatorChar) {
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

    private static String dataRecordToString(Object[] values, boolean statColumns, int aggregatedDataIndex, char separatorChar,
                                             DateFormat dateFormat) {
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
                    } else if (aggregatedDataIndex >= 0) {
                        sb.append(String.valueOf(aggregatedNumber.data[aggregatedDataIndex]));
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


    private static int getCommonDataArrayLength(Object[] attributeValues) {
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
