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

package com.bc.calvalus.processing.l3.cellstream;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * An input format specific for file containing cell files.
 * It is capable of transferring metadata from the input format to the mapper
 *
 * @author MarcoZ
 */
public class CellFileSplit extends FileSplit {

    private Map<String, String> metadata; // not serialized

    /**
     * For deserialize only!
     */
    CellFileSplit() {
        super(null, 0, 0, null);
    }

    /**
     * Constructs a split with host information
     *
     * @param fileSplit  the file split
     */
    public CellFileSplit(FileSplit fileSplit) throws IOException {
        super(fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength(), fileSplit.getLocations());
    }

    public Map<String, String> getMetadata() {
        if (metadata == null) {
            metadata = new HashMap<String, String>();
        }
        return metadata;
    }

    public boolean hasMetadata() {
        return  metadata != null;
    }
}
