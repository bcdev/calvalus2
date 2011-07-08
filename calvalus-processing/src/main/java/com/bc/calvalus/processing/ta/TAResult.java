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

import com.bc.calvalus.binning.Vector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * The result of the trend analysis.
 *
 * @author Norman
 */
public class TAResult {

    private HashMap<String, ArrayList<Record>> tables = new HashMap<String, ArrayList<Record>>();
    private final String[][] outputFeatureNames;
    private final int aggregatorCount;

    public TAResult(int aggregatorCount) {
        this.aggregatorCount = aggregatorCount;
        this.outputFeatureNames = new String[aggregatorCount][0];
    }

    public int getAggregatorCount() {
        return aggregatorCount;
    }

    public void setOutputFeatureNames(int i, String[] outputFeatureNames) {
        this.outputFeatureNames[i] = outputFeatureNames;
    }

    public String[] getOutputFeatureNames(int index) {
        return outputFeatureNames[index];
    }

    public Set<String> getRegionNames() {
        return tables.keySet();
    }

    public void addRecord(String regionName, String startDate, String stopDate, Vector outputVector) {
        ArrayList<Record> records = tables.get(regionName);
        if (records == null) {
            records = new ArrayList<Record>();
            tables.put(regionName, records);
        }
        records.add(new Record(startDate, stopDate, outputVector));
    }

    public List<Record> getRecords(String regionName) {
        ArrayList<Record> records = tables.get(regionName);
        if (records == null) {
            return null;
        }
        Collections.sort(records, new Comparator<Record>() {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.startDate.compareTo(o2.startDate);
            }
        });
        return Collections.unmodifiableList(records);
    }

    public static class Record {
        public final String startDate;
        public final String stopDate;
        public final Vector outputVector;

        public Record(String startDate, String stopDate, Vector outputVector) {
            this.startDate = startDate;
            this.stopDate = stopDate;
            this.outputVector = outputVector;
        }
    }
}
