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

package com.bc.calvalus.production;

import com.bc.calvalus.inventory.FileSystemService;
import org.apache.hadoop.fs.FileStatus;
import org.junit.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

@Ignore
public class TestFileSystemService implements FileSystemService {

    @Override
    public String[] globPaths(String username, List<String> pathPatterns) {
        String[] inputPathes = new String[pathPatterns.size()];
        for (int i = 0; i < pathPatterns.size(); i++) {
            String inputRegex = pathPatterns.get(i);
            if (inputRegex.contains("*")) {
                throw new IllegalArgumentException("Hey, wildcards are not supported! This is a test class!");
            }
            if (!inputRegex.startsWith("/")) {
                inputRegex = "/calvalus/eodata/" + inputRegex;
            }
            inputPathes[i] = "hdfs://master00:9000" + inputRegex;
        }
        return inputPathes;
    }

    @Override
    public String getQualifiedPath(String username, String outputPath) {
        if (!outputPath.startsWith("/")) {
            outputPath = "/calvalus/outputs/" + outputPath;
        }
        return "hdfs://master00:9000" + outputPath;
    }

    @Override
    public OutputStream addFile(String username, String path) throws IOException {
        return null;
    }

    @Override
    public boolean removeFile(String username, String userPath) throws IOException {
        return false;
    }

    @Override
    public boolean removeDirectory(String username, String userPath) throws IOException {
        return false;
    }

    @Override
    public boolean pathExists(String path, String username) throws IOException {
        return false;
    }

    @Override
    public InputStream openFile(String username, String path) {
        return null;
    }

    @Override
    public FileStatus[] globFiles(String username, List<String> pathPatterns) throws IOException {
        return new FileStatus[0];
    }
}
