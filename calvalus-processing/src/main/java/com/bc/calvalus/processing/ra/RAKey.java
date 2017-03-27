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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A {@link org.apache.hadoop.io.Writable} to hold a region analysis extract key.
 *
 * @author MarcoZ
 */
public class RAKey implements WritableComparable<RAKey> {

    private IntWritable regionId;
    private Text regionName;
    private LongWritable time;

    public RAKey() {
        regionId = new IntWritable();
        regionName = new Text();
        time = new LongWritable();
    }

    public RAKey(int regionIndxe, String regionName, long time) {
        this.regionId = new IntWritable(regionIndxe);
        this.regionName = new Text(regionName);
        this.time = new LongWritable(time);
    }

    public int getRegionId() {
        return regionId.get();
    }

    public String getRegionName() {
        return regionName.toString();
    }

    public long getTime() {
        return time.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        time.write(out);
        regionId.write(out);
        regionName.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        time.readFields(in);
        regionId.readFields(in);
        regionName.readFields(in);
    }

    @Override
    public int compareTo(RAKey that) {
        int compare = Integer.compare(getRegionId(), that.getRegionId());
        if (compare == 0) {
            compare = Long.compare(getTime(), that.getTime());
        }
        return compare;
    }


    public static class GroupComparator extends WritableComparator {

        protected GroupComparator() {
            super(RAKey.class, true);
        }

        @Override
        public int compare(WritableComparable key1, WritableComparable key2) {
            RAKey thisKey = (RAKey) key1;
            RAKey thatKey = (RAKey) key2;
            return Integer.compare(thisKey.getRegionId(), thatKey.getRegionId());
        }
    }

}
