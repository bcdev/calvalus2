/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.ta;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Writes CSV reports for trend analysis.
 */
public class TAReport {

    private final TAResult taResult;

    public TAReport(TAResult taResult) {
        this.taResult = taResult;
    }

    public void writeRegionCsvReport(Writer writer, String regionName) throws IOException {
        try {
            writeHeader(writer);
            List<TAResult.Record> records = taResult.getRecords(regionName);
            for (TAResult.Record record : records) {
                writeRecord(writer, record);
            }
        } finally {
            writer.close();
        }
    }

    private void writeRecord(Writer writer, TAResult.Record record) throws IOException {
        writer.write(record.startDate);
        writer.write("\t");
        writer.write(record.stopDate);
        for (int i = 0; i < record.outputVector.size(); i++) {
            writer.write("\t");
            writer.write(record.outputVector.get(i) + "");
        }
        writer.write("\n");
    }

    private void writeHeader(Writer writer) throws IOException {
        List<String> header = getHeader();
        for (String s : header) {
            writer.write(s);
            writer.write("\t");
        }
        writer.write("\n");
    }

    private List<String> getHeader() {
        List<String> header = new ArrayList<String>();
        header.add("start_date");
        header.add("stop_date");
        int aggregatorCount = taResult.getAggregatorCount();
        for (int i = 0; i < aggregatorCount; i++) {
            String[] outputFeatureNames = taResult.getOutputFeatureNames(i);
            header.addAll(Arrays.asList(outputFeatureNames));
        }
        return header;
    }

}
