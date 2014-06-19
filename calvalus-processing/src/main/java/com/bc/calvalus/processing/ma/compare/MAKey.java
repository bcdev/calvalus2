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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author MarcoZ
 */
public class MAKey implements WritableComparable<MAKey> {

    static final int HEADER_KEY = -1;
    static final int INSITU_IDENTIFIER = -1;

    private IntWritable referenceId; //header = -1
    private Text productName;
    private IntWritable identifierOrder; //

    public MAKey() {
        referenceId = new IntWritable();
        productName = new Text();
        identifierOrder = new IntWritable();
    }

    public int getReferenceId() {
        return referenceId.get();
    }

    public void setReferenceId(int referenceId) {
        this.referenceId.set(referenceId);
    }

    public String getProductName() {
        return productName.toString();
    }

    public void setProductName(String productName) {
        this.productName.set(productName);
    }

    public int getIdentifierOrder() {
        return identifierOrder.get();
    }

    public void setIdentifierOrder(int identifierOrder) {
        this.identifierOrder.set(identifierOrder);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        referenceId.write(out);
        productName.write(out);
        identifierOrder.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        referenceId.readFields(in);
        productName.readFields(in);
        identifierOrder.readFields(in);
    }

    @Override
    public int compareTo(MAKey other) {
        int compare = compareInts(getReferenceId(), other.getReferenceId());
        if (compare == 0) {
            compare = getProductName().compareTo(other.getProductName());
            if (compare == 0) {
                compare = compareInts(getIdentifierOrder(), other.getIdentifierOrder());
            }
        }
        return compare;
    }

    static int compareInts(int thisInt, int thatInt) {
        if (thisInt < thatInt) {
            return -1;
        } else if (thisInt == thatInt) {
            return 0;
        } else {
            return 1;
        }
    }

    public static class GroupingComparator extends WritableComparator {

        protected GroupingComparator() {
            super(MAKey.class, true);
        }

        @Override
        public int compare(WritableComparable key1, WritableComparable key2) {
            MAKey thisKey = (MAKey)key1;
            MAKey thatKey = (MAKey)key2;
            int compare = compareInts(thisKey.getReferenceId(), thatKey.getReferenceId());
            if (compare == 0) {
                compare = thisKey.getProductName().compareTo(thatKey.getProductName());
            }
            return compare;
        }
    }
}
