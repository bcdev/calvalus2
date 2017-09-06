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

package com.bc.calvalus.inventory.hadoop;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.security.AccessControlException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * A FileSystem that uses an external program to do ls and cat as user via setuid.
 *
 * @author boe
 */
public class CalvalusShFileSystem extends FileSystem {

    private static final Logger LOG = CalvalusLogger.getLogger();
    public static final String CALVALUS_SH_COMMAND = "/usr/libexec/hadoop/calvalus-sh";

    protected final String username;
    protected Path workingDir = null;
    protected JobClientsMap.CacheEntry cacheEntry;
    protected FileSystem unixFileSystem;


    public CalvalusShFileSystem(String username, JobClientsMap.CacheEntry cacheEntry) throws IOException {
        super();
        this.username = username;
        this.cacheEntry = cacheEntry;
        unixFileSystem = cacheEntry.getJobClientInternal().getFs();
    }

    public JobClientsMap.CacheEntry getCacheEntry() {
        return cacheEntry;
    }

    public FSDataInputStream open(Path f) throws IOException {
      return open(f, 4096);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        String p = path.toString();
        if (p.startsWith("file://")) {
            p = p.substring("file://".length());
        } else if (p.startsWith("file:")) {
            p = p.substring("file:".length());
        }
        ProcessBuilder pb = new ProcessBuilder(CALVALUS_SH_COMMAND, username, "cat", p);
        LOG.fine("calling " + CALVALUS_SH_COMMAND + " " + username + " cat " + p);
        pb.redirectErrorStream(true);
        LOG.fine("file " + p + " externally opened for reading");
        Process proc = pb.start();
//        try {
//            proc.waitFor();
//        } catch (InterruptedException e) {}
        return new DummyFSDataInputStream(proc.getInputStream());
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        String p = path.toString();
        if (p.startsWith("file://")) {
            p = p.substring("file://".length());
        } else if (p.startsWith("file:")) {
            p = p.substring("file:".length());
        }
        ProcessBuilder pb = new ProcessBuilder(CALVALUS_SH_COMMAND, username, "ls -1p", p);
        pb.redirectErrorStream(true);
        Process proc = pb.start();

        List<FileStatus> files = new ArrayList<FileStatus>();
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                boolean isDir = line.endsWith("/");
                if (isDir) {
                    line = line.substring(0, line.length() - 1);
                }
                if (! line.equals(p)) {
                    line = p + "/" + line;
                }
                files.add(new FileStatus(0, isDir, 1, 0, 0, new Path(line)));
            }
        }

        try {
            int code = proc.waitFor();
            if (code == 0) {
                LOG.fine("path " + path.toString() + " externally listed, " + files.size() + " entries");
                return files.toArray(new FileStatus[files.size()]);
            } else if (code == 2) {
                LOG.info("path " + path.toString() + " externally listed, access denied");
                throw new AccessControlException(p);
            }
        } catch (InterruptedException _) {}
        LOG.fine("path " + path.toString() + " externally listed, not found");
        return new FileStatus[0];
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        FileStatus[] files = listStatus(f);
        if (files.length < 1) {
            throw new FileNotFoundException(f.toString());
        }
        return files[0];
    }

    @Override
    public FileStatus[] globStatus(Path path) throws IOException {
        String p = path.toString();
        if (p.startsWith("file://")) {
            p = p.substring("file://".length());
        } else if (p.startsWith("file:")) {
            p = p.substring("file:".length());
        }
        ProcessBuilder pb = new ProcessBuilder(CALVALUS_SH_COMMAND, username, "stat -L -c %F#%n", p);
        pb.redirectErrorStream(true);
        Process proc = pb.start();

        List<FileStatus> files = new ArrayList<FileStatus>();
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                boolean isDir = line.startsWith("directory#");
                int pos = line.indexOf('#');
                if (pos == -1) {
                    continue;
                }
                line = line.substring(pos+1);
                files.add(new FileStatus(0, isDir, 1, 0, 0, new Path(line)));
            }
        }

        try {
            int code = proc.waitFor();
            if (code == 0) {
                LOG.fine("path " + path.toString() + " externally listed, " + files.size() + " entries");
                return files.toArray(new FileStatus[files.size()]);
            } else if (code == 2) {
                LOG.fine("path " + path.toString() + " externally listed, access denied");
                throw new AccessControlException(p);
            }
        } catch (InterruptedException _) {}
        LOG.fine("path " + path.toString() + " externally listed, not found");
        return new FileStatus[0];
    }


    @Override
    public URI getUri() {
        return unixFileSystem.getUri();
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        unixFileSystem.setWorkingDirectory(new_dir);
    }

    @Override
    public Path getWorkingDirectory() {
        return unixFileSystem.getWorkingDirectory();
    }

    @Override
    public Path makeQualified(Path path) {
        return unixFileSystem.makeQualified(path);
    }


    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        throw new NotImplementedException();
    }
}
