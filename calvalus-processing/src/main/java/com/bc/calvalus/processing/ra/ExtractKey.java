/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.ra;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static sun.management.snmp.jvminstr.JvmThreadInstanceEntryImpl.ThreadStateMap.Byte1.other;

/**
 * A {@link org.apache.hadoop.io.Writable} to hold a region analysis extract key.
 *
 * @author MarcoZ
 */
public class ExtractKey implements WritableComparable<ExtractKey> {

    private LongWritable time;
    private IntWritable regionId;

    public ExtractKey() {
        time = new LongWritable();
        regionId = new IntWritable();
    }

    public int getRegionId() {
        return regionId.get();
    }

    public void setRegionId(int regionId) {
        this.regionId.set(regionId);
    }

    public long getTime() {
        return time.get();
    }

    public void setTime(long time) {
        this.time.set(time);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        time.write(out);
        regionId.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        time.readFields(in);
        regionId.readFields(in);
    }

    @Override
    public int compareTo(ExtractKey that) {
        int compare = Integer.compare(getRegionId(), that.getRegionId());
        if (compare == 0) {
            compare = Long.compare(getTime(), that.getTime());
        }
        return compare;
    }


    public static class GroupComparator extends WritableComparator {

        protected GroupComparator() {
            super(ExtractKey.class, true);
        }

        @Override
        public int compare(WritableComparable key1, WritableComparable key2) {
            ExtractKey thisKey = (ExtractKey) key1;
            ExtractKey thatKey = (ExtractKey) key2;
            return Integer.compare(thisKey.getRegionId(), thatKey.getRegionId());
        }
    }

}
