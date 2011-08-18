package com.bc.calvalus.processing.ma;

import com.bc.ceres.core.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A default implementation of a {@link RecordSource}.
 * Its main purpose is testing.
 *
 * @author Norman
 */
public class DefaultRecordSource implements RecordSource {

    private final Header header;
    private final List<Record> records;

    public DefaultRecordSource(Header header, Record... records) {
        Assert.notNull(header, "header");
        this.header = header;
        this.records = new ArrayList<Record>(Arrays.asList(records));
    }

    public void addRecord(Record record) {
        records.add(record);
    }

    @Override
    public Header getHeader() {
        return header;
    }

    @Override
    public Iterable<Record> getRecords() {
        return Collections.unmodifiableList(records);
    }
}
