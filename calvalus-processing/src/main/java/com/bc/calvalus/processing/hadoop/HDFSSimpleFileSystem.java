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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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

    private final FileSystem fileSystem;
    private final TaskInputOutputContext<?, ?, ?, ?> context;

    public HDFSSimpleFileSystem(TaskInputOutputContext<?, ?, ?, ?> context) throws IOException {
        this.context = context;
        Configuration configuration = context.getConfiguration();
        fileSystem = FileSystem.get(configuration);
    }

    @Override
    public Reader createReader(String path) throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path(path));
        return new InputStreamReader(inputStream);
    }

    @Override
    public Writer createWriter(String path) throws IOException {
        System.out.println("HDFSSimpleFileSystem.createWriter");
        System.out.println("path = [" + path + "]");
        Path workOutputPath;
        try {
            workOutputPath = FileOutputFormat.getWorkOutputPath(context);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        Path workPath = new Path(workOutputPath, path);
        System.out.println("workPath = " + workPath);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(workPath);
        return new OutputStreamWriter(fsDataOutputStream);
    }

    @Override
    public String[] list(String path) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
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
    public boolean isFile(String s) {
        try {
            return fileSystem.isFile(new Path(s));
        } catch (IOException ignore) {
            return false;
        }
    }
}
