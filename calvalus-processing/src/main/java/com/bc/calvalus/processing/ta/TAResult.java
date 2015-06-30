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

import org.esa.snap.binning.Vector;

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

    private HashMap<String, ArrayList<Record>> dateRecords = new HashMap<String, ArrayList<Record>>();
    private String[] outputFeatureNames;

    public TAResult() {
    }

    public void setOutputFeatureNames(String... outputFeatureNames) {
        this.outputFeatureNames = outputFeatureNames;
    }

    public String[] getOutputFeatureNames() {
        return outputFeatureNames;
    }

    public Set<String> getRegionNames() {
        return dateRecords.keySet();
    }

    public void addRecord(String regionName, String startDate, String stopDate, Vector outputVector) {
        ArrayList<Record> records = dateRecords.get(regionName);
        if (records == null) {
            records = new ArrayList<Record>();
            dateRecords.put(regionName, records);
        }
        records.add(new Record(startDate, stopDate, outputVector));
    }

    public List<Record> getRecords(String regionName) {
        ArrayList<Record> records = dateRecords.get(regionName);
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
