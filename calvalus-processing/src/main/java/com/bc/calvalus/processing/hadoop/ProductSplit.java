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

package com.bc.calvalus.processing.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * An input format specific for EO data products,
 * that contain the first and last raster line that should be processed.
 *
 * @author MarcoZ
 */
public class ProductSplit extends FileSplit {

    private int processStartLine;
    private int processLength;

    /**
     * For deserialize only!
     */
    ProductSplit() {
        super(null, 0, 0, null);
    }

    /**
     * Constructs a split with host information for the whole product
     *
     * @param file      the file name
     * @param length    the number of bytes in the file to process
     * @param hosts     the list of hosts containing the block, possibly null
     */
    public ProductSplit(Path file, long length, String[] hosts) {
        this(file, length, hosts, -1, -1);
    }

    /**
     * Constructs a split with host information for a part of the product
     *
     * @param file      the file name
     * @param length    the number of bytes in the file to process
     * @param hosts     the list of hosts containing the block, possibly null
     * @param processStart the first line that should be processed
     * @param processLength  the last line that should be processed
     */
    public ProductSplit(Path file, long length, String[] hosts, int processStart, int processLength) {
        super(file, 0, length, hosts);
        this.processStartLine = processStart;
        this.processLength = processLength;
    }

    public int getProcessStartLine() {
        return processStartLine;
    }

    public int getProcessLength() {
        return processLength;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(processStartLine);
        out.writeInt(processLength);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        processStartLine = in.readInt();
        processLength = in.readInt();
    }
}
