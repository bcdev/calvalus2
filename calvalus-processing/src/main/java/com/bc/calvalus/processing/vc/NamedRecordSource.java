/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.vc;

import com.bc.calvalus.processing.ma.Header;
import com.bc.calvalus.processing.ma.Record;
import com.bc.calvalus.processing.ma.RecordSource;
import com.bc.ceres.core.Assert;

import java.util.Collections;
import java.util.List;

/**
 * a named record source
 */
public class NamedRecordSource implements RecordSource {

    private final String name;
    private final Header header;
    private final List<Record> records;

    public NamedRecordSource(String name, Header header, List<Record> records) {
        Assert.notNull(name, "name");
        Assert.notNull(header, "header");
        this.name = name;
        this.header = header;
        this.records = records;
    }

    public String getName() {
        return name;
    }

    public int getNumRecords() {
        return records.size();
    }

    @Override
    public Header getHeader() {
        return header;
    }

    @Override
    public Iterable<Record> getRecords() {
        return Collections.unmodifiableList(records);
    }

    @Override
    public String getTimeAndLocationColumnDescription() {
        return null;
    }
}
