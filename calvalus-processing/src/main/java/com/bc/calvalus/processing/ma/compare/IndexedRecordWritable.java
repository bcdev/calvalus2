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

package com.bc.calvalus.processing.ma.compare;

import com.bc.calvalus.processing.ma.RecordWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A record writable together with an index
 */
public class IndexedRecordWritable extends RecordWritable {

    private IntWritable identifierIndex;
    private RecordWritable recordWritable;

    public IndexedRecordWritable() {
        identifierIndex = new IntWritable();
        recordWritable = new RecordWritable();
    }

    public IndexedRecordWritable(int identifierIndex, Object[] newAttributeNames, Object[] annotationValues) {
        super(newAttributeNames, annotationValues);
        this.identifierIndex = new IntWritable(identifierIndex);
    }

    public int getIdentifierIndex() {
        return identifierIndex.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        identifierIndex.write(out);
        recordWritable.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        identifierIndex.readFields(in);
        recordWritable.readFields(in);
    }
}
