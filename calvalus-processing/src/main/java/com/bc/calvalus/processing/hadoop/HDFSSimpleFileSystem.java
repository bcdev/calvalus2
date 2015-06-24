/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import com.bc.ceres.metadata.SimpleFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

/**
 * A simple filesystem implementation using HDFS for metadata and resource processing.
 */
public class HDFSSimpleFileSystem implements SimpleFileSystem {

    private final TaskInputOutputContext<?, ?, ?, ?> context;
    private final Configuration conf;

    public HDFSSimpleFileSystem(TaskInputOutputContext<?, ?, ?, ?> context) throws IOException {
        this.context = context;
        this.conf = context.getConfiguration();
    }

    @Override
    public Reader createReader(String pathString) throws IOException {
        final Path path = new Path(pathString);
        FileSystem fs = path.getFileSystem(conf);
        return new InputStreamReader(fs.open(path));
    }

    @Override
    public Writer createWriter(String path) throws IOException {
        Path workOutputPath;
        try {
            workOutputPath = FileOutputFormat.getWorkOutputPath(context);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        Path workPath = new Path(workOutputPath, path);
        FileSystem fs = workPath.getFileSystem(conf);
        return new OutputStreamWriter(fs.create(workPath));
    }

    @Override
    public String[] list(String pathString) throws IOException {
        Path path = new Path(pathString);
        FileSystem fs = path.getFileSystem(conf);
        if (!fs.exists(path)) {
            return null;
        }
        FileStatus[] fileStatuses = fs.listStatus(path);
        if (fileStatuses == null) {
            return null;
        }
        String[] names = new String[fileStatuses.length];
        for (int i = 0; i < fileStatuses.length; i++) {
            names[i] = fileStatuses[i].getPath().getName();
        }
        return names;
    }

    @Override
    public boolean isFile(String pathString) {
        try {
            Path path = new Path(pathString);
            FileSystem fs = path.getFileSystem(conf);
            return fs.isFile(path);
        } catch (IOException ignore) {
            return false;
        }
    }
}
