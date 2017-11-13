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

import com.bc.calvalus.processing.hadoop.FSImageInputStream;
import com.bc.inventory.search.StreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import javax.imageio.stream.ImageInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * An implementation using the Hadoop FileSystem
 */
class HDFSStreamFactory implements StreamFactory {

    private final Configuration conf;

    HDFSStreamFactory(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public ImageInputStream createImageInputStream(String path) throws IOException {
        Path hdfsPath = new Path(path);
        FileSystem fs = hdfsPath.getFileSystem(conf);
        FileStatus status = fs.getFileStatus(hdfsPath);
        FSDataInputStream in = fs.open(hdfsPath);
        return new FSImageInputStream(in, status.getLen());
    }

    @Override
    public InputStream createInputStream(String path) throws IOException {
        Path hdfsPath = new Path(path);
        return hdfsPath.getFileSystem(conf).open(hdfsPath);
    }

    @Override
    public OutputStream createOutputStream(String path) throws IOException {
        Path hdfsPath = new Path(path);
        FileSystem fs = hdfsPath.getFileSystem(conf);
        // for geo db files use replication of 3, so they are safer
        return fs.create(hdfsPath, true,
                  conf.getInt("io.file.buffer.size", 4096),
                  (short) 3,
                  fs.getDefaultBlockSize(hdfsPath));
    }

    @Override
    public boolean exists(String path) throws IOException {
        Path hdfsPath = new Path(path);
        FileSystem fs = hdfsPath.getFileSystem(conf);
        return fs.exists(hdfsPath);
    }

    @Override
    public String[] listNewestFirst(String... filenames) throws IOException {
        if (filenames.length == 0) {
            return new String[0];
        }
        List<FileStatus> existingFiles = new ArrayList<>(filenames.length);
        for (String filename : filenames) {
            Path path = new Path(filename);
            try {
                // filename are always files, never directories
                FileStatus status = path.getFileSystem(conf).getFileStatus(path);
                existingFiles.add(status);
//              current impl. of CalvalusShFileSystem does not support listStatus of files, only of directories
//                FileStatus[] statuses = path.getFileSystem(conf).listStatus(path);
//                if (statuses.length > 1) {
//                    String msg = String.format("list of (%s) results in multiple results: %s",
//                                               path, Arrays.toString(statuses));
//                    throw new IOException(msg);
//                }
//                existingFiles.add(statuses[0]);
            } catch (IOException ioe) {
                // this file may no exits,ignore it
            }
        }
        existingFiles.sort(Comparator.comparingLong(FileStatus::getModificationTime));
        Collections.reverse(existingFiles);
        // return existingFiles.stream().map(f -> f.getPath().toString()).toArray(String[]::new); ?
        String[] result = new String[existingFiles.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = existingFiles.get(i).getPath().toString();
        }
        return result;
    }

    @Override
    public void rename(String oldName, String newName) throws IOException {
        Path hdfsPathOld = new Path(oldName);
        Path hdfsPathNew = new Path(newName);
        FileSystem fs = hdfsPathOld.getFileSystem(conf);
        if (fs.exists(hdfsPathNew)) {
            fs.delete(hdfsPathNew, false);
        }
        fs.rename(hdfsPathOld, hdfsPathNew);
    }

    @Override
    public String[] listWithPrefix(String dir, String prefix) throws IOException {
        Path hdfsPathDir = new Path(dir);
        FileSystem fs = hdfsPathDir.getFileSystem(conf);
        FileStatus[] fileStatuses;
        try {
            fileStatuses = fs.listStatus(hdfsPathDir, path -> path.getName().startsWith(prefix));
        } catch (FileNotFoundException e) {
            fileStatuses = new FileStatus[0];
        }

        String[] result = new String[fileStatuses.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = fileStatuses[i].getPath().toString();
        }
        return result;
    }

    @Override
    public void concat(String[] sourceFilenames, String destFilename) throws IOException {
        try (OutputStream os = createOutputStream(destFilename)) {
            for (String sourceFilename : sourceFilenames) {
                Path path = new Path(sourceFilename);
                try (FSDataInputStream is = path.getFileSystem(conf).open(path)) {
                    IOUtils.copyBytes(is, os, conf, false);
                }
            }
        }
    }

    @Override
    public void delete(String filename) throws IOException {
        Path hdfsPath = new Path(filename);
        FileSystem fs = hdfsPath.getFileSystem(conf);
        fs.delete(hdfsPath, false);
    }
}
