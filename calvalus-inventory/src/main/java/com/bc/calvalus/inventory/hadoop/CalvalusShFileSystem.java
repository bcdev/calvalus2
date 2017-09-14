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
        String p = getUnixPath(path);
        Process proc = callUnixCommand("cat", p);
        LOG.fine("file " + p + " externally opened for reading");
        return new DummyFSDataInputStream(proc.getInputStream());
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        String p = getUnixPath(path);
        Process proc = callUnixCommand("ls -l", p);
        List<FileStatus> files = collectPathsOutput(proc);
        handleReturnCode(proc, files, path);
        return files.toArray(new FileStatus[files.size()]);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        String p = getUnixPath(path);
        Process proc = callUnixCommand("stat -l", p);
        List<FileStatus> files = collectPathsOutput(proc);
        handleReturnCode(proc, files, path);
        if (files.size() < 1) {
            throw new FileNotFoundException(path.toString());
        }
        return files.get(0);
    }

    @Override
    public FileStatus[] globStatus(Path path) throws IOException {
        String p = getUnixPath(path);
        Process proc = callUnixCommand("glob", p);
        List<FileStatus> files = collectPathsOutput(proc);
        handleReturnCode(proc, files, path);
        return files.toArray(new FileStatus[files.size()]);
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        String p = getUnixPath(path);
        Process proc = callUnixCommand("mkdirs", p, String.format("%o", permission.toShort()));
        LOG.fine("dirs " + p + " externally created");
        return waitForReturnCode(proc, path) == 0;
    }

    private String getUnixPath(Path path) {
        String p = path.toString();
        if (p.startsWith("file://")) {
            p = p.substring("file://".length());
        } else if (p.startsWith("file:")) {
            p = p.substring("file:".length());
        }
        return p;
    }

    private Process callUnixCommand(String cmd, String... path) throws IOException {
        ProcessBuilder pb = path.length == 1
                ? new ProcessBuilder(CALVALUS_SH_COMMAND, username, cmd, path[0])
                : new ProcessBuilder(CALVALUS_SH_COMMAND, username, cmd, path[0], path[1]);
        pb.redirectErrorStream(true);
        Process proc = pb.start();
        LOG.fine("calling " + CALVALUS_SH_COMMAND + " " + username + " " + cmd + " " + path[0]);
        return proc;
    }

    private List<FileStatus> collectPathsOutput(Process proc) throws IOException {
        List<FileStatus> files = new ArrayList<FileStatus>();
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] token = line.split("\t");
                boolean isDir = token[0].endsWith("/");
                String path = isDir ? token[0].substring(0, token[0].length()-1) : token[0];
                if (token.length >= 3) {
                    long length = Long.parseLong(token[1]);
                    long mtime = Long.parseLong(token[2]);
                    files.add(new FileStatus(length, isDir, 1, length, mtime, new Path(path)));
                } else {
                    files.add(new FileStatus(1, isDir, 1, 1, 0, new Path(path)));
                }
            }
        }
        return files;
    }

    private void handleReturnCode(Process proc, List<FileStatus> files, Path path) throws AccessControlException {
        try {
            int code = proc.waitFor();
            switch (code) {
                case 0:
                    LOG.fine("path " + path.toString() + " externally listed, " + files.size() + " entries");
                    break;
                case 2:
                    LOG.fine("path " + path.toString() + " externally listed, access denied");
                    throw new AccessControlException(path.toString());
                case 3:
                    LOG.fine("path " + path.toString() + " externally listed, not found");
                    files.clear();
                    break;
                default:
                    LOG.warning("path " + path.toString() + " externally listed, listing failed");
                    files.clear();
            }
        } catch (InterruptedException _) {
            LOG.fine("path " + path.toString() + " externally listed, interrupted");
            files.clear();
        }
    }

    private int waitForReturnCode(Process proc, Path path) throws AccessControlException {
        try {
            int code = proc.waitFor();
            switch (code) {
                case 2:
                    LOG.fine("path " + path.toString() + " access denied");
                    throw new AccessControlException(path.toString());
                default:
                    return code;
            }
        } catch (InterruptedException _) {
            LOG.fine("path " + path.toString() + " interrupted");
            return 1;
        }

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
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        String p = getUnixPath(path);
        Process proc = callUnixCommand("create", p, String.format("%o", permission.toShort()));
        LOG.fine("file " + p + " externally opened for writing");
        return new FSDataOutputStream(proc.getOutputStream());
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
