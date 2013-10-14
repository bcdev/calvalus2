/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.StringUtils;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CsvLineReader {

    private static final String[] LAT_NAMES = new String[]{"lat", "latitude", "northing"};
    private static final String[] LON_NAMES = new String[]{"lon", "long", "longitude", "easting"};
    private static final String[] TIME_NAMES = new String[]{"time", "date"};

    private final LineNumberReader lineReader;
    private final String[] attributeNames;
    private final int latIndex;
    private final int lonIndex;
    private final int timeIndex;
    private final int[] timeIndices;
    private final DateFormat dateFormat;
    private final String columnSeparator;

    CsvLineReader(Reader reader, DateFormat dateFormat) throws IOException {
        if (reader instanceof LineNumberReader) {
            lineReader = (LineNumberReader) reader;
        } else {
            lineReader = new LineNumberReader(reader);
        }
        lineReader.mark(10000);
        Map<String, String> headerParams = readHeaderParameters(lineReader);
        lineReader.reset();
        if (headerParams.containsKey("columnSeparator")) {
            columnSeparator = headerParams.get("columnSeparator");
        } else {
            columnSeparator = "\t";
        }
        attributeNames = readTextRecord(-1);
        latIndex = getIndex(headerParams, "latColumn", LAT_NAMES);
        lonIndex = getIndex(headerParams, "lonColumn", LON_NAMES);

        if (headerParams.containsKey("timeColumn")) {
            timeIndex = TextUtils.indexOf(attributeNames, headerParams.get("timeColumn"));
            timeIndices = null;
        } else if (headerParams.containsKey("timeColumns")) {
            String[] timeColumns = headerParams.get("timeColumns").split(",");
            timeIndex = -1;
            timeIndices = new int[timeColumns.length];
            for (int i = 0; i < timeColumns.length; i++) {
                timeIndices[i] = TextUtils.indexOf(attributeNames, timeColumns[i]);
            }
            if (!headerParams.containsKey("dateFormat")) {
                throw new IllegalArgumentException("header parameter 'timeColumns' requires a user supplied 'dateFormat'.");
            }
        } else {
            timeIndex = TextUtils.indexOf(attributeNames, TIME_NAMES);
            timeIndices = null;
        }
        if (headerParams.containsKey("dateFormat")) {
            this.dateFormat = ProductData.UTC.createDateFormat(headerParams.get("dateFormat"));
        } else {
            this.dateFormat = dateFormat;
        }
    }

    public boolean hasTime() {
        return timeIndices != null || timeIndex != -1;
    }

    public Date extractTime(Object[] values, int lineNumber) {
        if (timeIndex != -1) {
            String timeAsString = String.valueOf(values[timeIndex]);
            try {
                return dateFormat.parse(timeAsString);
            } catch (ParseException e) {
                throw new IllegalArgumentException("time value '" + timeAsString
                                                           + "' in line " + lineNumber + " column " + timeIndex
                                                           + " of point data file not well-formed (pattern "
                                                           + (dateFormat instanceof SimpleDateFormat ? ((SimpleDateFormat) dateFormat).toPattern() : dateFormat.toString())
                                                           + " expected)");
            }
        } else if (timeIndices != null) {
            String[] timeComponents = new String[timeIndices.length];
            for (int i = 0; i < timeComponents.length; i++) {
                Object value = values[timeIndices[i]];
                if (value instanceof Double) {
                    timeComponents[i] = String.valueOf(((Double) value).intValue());
                } else if (value instanceof String) {
                    timeComponents[i] = (String) value;
                } else {
                    throw new IllegalArgumentException("unexpected argument type.");
                }
            }
            String fullTimeString = StringUtils.join(timeComponents, "|");
            try {
                return dateFormat.parse(fullTimeString);
            } catch (ParseException e) {
                throw new IllegalArgumentException("time value '" + fullTimeString
                                                           + "' in line " + lineNumber + " columns " + Arrays.toString(timeIndices)
                                                           + " of point data file not well-formed (pattern "
                                                           + (dateFormat instanceof SimpleDateFormat ? ((SimpleDateFormat) dateFormat).toPattern() : dateFormat.toString())
                                                           + " expected)");
            }
        } else {
            throw new IllegalStateException("No time information expected");
        }
    }

    private int getIndex(Map<String, String> headerParams, String keyName, String[] testValues) {
        if (headerParams.containsKey(keyName)) {
            return TextUtils.indexOf(attributeNames, headerParams.get(keyName));
        } else {
            return TextUtils.indexOf(attributeNames, testValues);
        }
    }

    public String[] getAttributeNames() {
        return attributeNames;
    }

    public String[] readTextRecord(int recordLength) throws IOException {
        String line;
        while ((line = lineReader.readLine()) != null) {
            String trimLine = line.trim();
            if (!trimLine.startsWith("#") && !trimLine.isEmpty()) {
                return splitRecordLine(line, recordLength);
            }
        }
        return null;
    }

    private String[] splitRecordLine(String line, int recordLength) {
        int pos2;
        int pos1 = 0;
        ArrayList<String> strings = new ArrayList<String>(256);
        while ((pos2 = line.indexOf(columnSeparator, pos1)) >= 0) {
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

    static Map<String, String> readHeaderParameters(LineNumberReader reader) throws IOException {
        int firstChar = reader.read();
        Map<String, String> parameters = new HashMap<String, String>();
        while (firstChar == '#') {
            String line = reader.readLine();
            if (line.contains("=")) {
                String[] keyValue = line.split("=");
                if (!keyValue[0].isEmpty() && !keyValue[1].isEmpty()) {
                    parameters.put(keyValue[0].trim(), keyValue[1].trim());
                }
            }
            firstChar = reader.read();
        }
        return parameters;
    }


    public int getLineNumber() {
        return lineReader.getLineNumber();
    }

    public int getLatIndex() {
        return latIndex;
    }

    public int getLonIndex() {
        return lonIndex;
    }

    public String getLatNames() {
        return StringUtils.join(LAT_NAMES, ", ");
    }

    public String getLonNames() {
        return StringUtils.join(LON_NAMES, ", ");
    }

    public String getTimeColumnNames() {
        if (timeIndex != -1) {
            return attributeNames[timeIndex];
        } else if (timeIndices != null) {
            String[] timeComponents = new String[timeIndices.length];
            for (int i = 0; i < timeComponents.length; i++) {
                timeComponents[i] = attributeNames[timeIndices[i]];
            }
            return StringUtils.join(timeComponents, ",");
        } else {
            return null;
        }
    }
}
