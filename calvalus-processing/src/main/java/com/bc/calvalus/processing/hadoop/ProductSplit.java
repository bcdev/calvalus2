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

    private int startLine;
    private int stopLine;

    /**
     * Constructs a split with host information
     *
     * @param file      the file name
     * @param length    the number of bytes in the file to process
     * @param hosts     the list of hosts containing the block, possibly null
     * @param startLine the first line that should be processed
     * @param stopLine  the last line that should be processed
     */
    public ProductSplit(Path file, long length, String[] hosts, int startLine, int stopLine) {
        super(file, 0, length, hosts);
        this.startLine = startLine;
        this.stopLine = stopLine;
    }

    public int getStartLine() {
        return startLine;
    }

    public int getStopLine() {
        return stopLine;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(startLine);
        out.writeInt(stopLine);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        startLine = in.readInt();
        stopLine = in.readInt();
    }

}
