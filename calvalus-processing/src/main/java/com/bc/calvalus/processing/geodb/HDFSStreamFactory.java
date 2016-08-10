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

package com.bc.calvalus.processing.geodb;

import com.bc.inventory.search.StreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * An implementation using the Hadoop FileSystem
 */
class HDFSStreamFactory implements StreamFactory {

    private final Path dbPath;
    private final Configuration conf;

    HDFSStreamFactory(String geoInventory, Configuration conf) throws IOException {
        Path path = new Path(geoInventory);
        this.dbPath = path.getFileSystem(conf).makeQualified(path);
        this.conf = conf;
    }

    @Override
    public InputStream createInputStream(String name) throws IOException {
        Path path = new Path(dbPath, name);
        FileSystem fs = path.getFileSystem(conf);
        return fs.open(path);
    }

    @Override
    public OutputStream createOutputStream(String name) throws IOException {
        Path path = new Path(dbPath, name);
        FileSystem fs = path.getFileSystem(conf);
        return fs.create(path, true);
    }

    @Override
    public boolean exists(String name) throws IOException {
        Path path = new Path(dbPath, name);
        FileSystem fs = path.getFileSystem(conf);
        return fs.exists(path);
    }

}
