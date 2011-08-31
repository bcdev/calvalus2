package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.ProductData;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * Various text utility functions.
 *
 * @author Norman
 */
public class TextUtils {

    public static final DateFormat DEFAULT_DATE_FORMAT = ProductData.UTC.createDateFormat("yyyy-MM-dd hh:mm:ss");
    public static final char DEFAULT_COLUMN_SEPARATOR_CHAR = '\t';

    public static int indexOf(String[] textValues, String[] possibleValues) {
        for (String possibleValue : possibleValues) {
            for (int index = 0; index < textValues.length; index++) {
                if (possibleValue.equalsIgnoreCase(textValues[index])) {
                    return index;
                }
            }
        }
        return -1;
    }

    public static String toString(Object[] values) {
        return toString(values, DEFAULT_COLUMN_SEPARATOR_CHAR, DEFAULT_DATE_FORMAT);
    }

    public static String toString(Object[] values, char separatorChar, DateFormat dateFormat) {
        if (values == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder(256);
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(separatorChar);
            }
            Object value = values[i];
            if (value instanceof Date) {
                if (dateFormat == null) {
                    dateFormat = DEFAULT_DATE_FORMAT;
                }
                sb.append(dateFormat.format((Date) value));
            } else if (value instanceof AggregatedNumber) {
                AggregatedNumber aggregatedNumber = (AggregatedNumber) value;
                sb.append(aggregatedNumber.mean);
                sb.append(separatorChar);
                sb.append(aggregatedNumber.sigma);
                sb.append(separatorChar);
                sb.append(aggregatedNumber.n);
            } else if (value != null) {
                sb.append(value.toString());
            }
        }

        return sb.toString();
    }
}
