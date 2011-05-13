package com.bc.calvalus.processing.ta;

import com.bc.calvalus.binning.Vector;

import java.util.ArrayList;
import java.util.Arrays;
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

    private ArrayList<String> header;
    private HashMap<String, ArrayList<Record>> tables = new HashMap<String, ArrayList<Record>>();

    public void setOutputFeatureNames(String... outputFeatureNames) {
        header.clear();
        header.add("start_date");
        header.add("stop_date");
        header.addAll(Arrays.asList(outputFeatureNames));
    }

    public List<String> getHeader() {
        return Collections.unmodifiableList(header);
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
