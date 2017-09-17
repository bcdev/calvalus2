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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemSetter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.security.AccessControlException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    protected JobClientsMap.CacheEntry cacheEntry = null;
    protected FileSystem unixFileSystem;


    public static synchronized void createOrRegister(String username, UserGroupInformation remoteUser, JobClientsMap.CacheEntry cacheEntry, Map<String, FileSystem> fileSystemMap) throws IOException {
        FileSystem fs = cacheEntry.getJobClientInternal().getFs();
        if (!(fs instanceof CalvalusShFileSystem)) {
            final Configuration conf = cacheEntry.getJobClientInternal().getConf();
            final CalvalusShFileSystem cfs = new CalvalusShFileSystem(username, cacheEntry);
            try {
                remoteUser.doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystemSetter.addFileSystemForTesting(FileSystem.getDefaultUri(conf), conf, cfs));
            } catch (InterruptedException ex) {
                throw new IOException(ex);
            }
        } else {
            ((CalvalusShFileSystem)fs).setCacheEntry(cacheEntry);
        }
        fileSystemMap.put(username, fs);
        LOG.info("file system with external access control registered and cached for user " + username);
    }

    public static synchronized FileSystem getCreate(URI uri, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(uri, conf);
        if (fs instanceof CalvalusShFileSystem) {
            LOG.info("file system with external access control pre-exists for " + ((CalvalusShFileSystem) fs).getUsername());
            return fs;
        }
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        final CalvalusShFileSystem cfs = new CalvalusShFileSystem(currentUser.getShortUserName(), fs);
        try {
            currentUser.doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystemSetter.addFileSystemForTesting(FileSystem.getDefaultUri(conf), conf, cfs));
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        }
        LOG.info("file system with external access control registered for user " + currentUser.getShortUserName());
        return cfs;
    }


    public CalvalusShFileSystem(String username, FileSystem fs) {
        this.username = username;
        this.unixFileSystem = fs;
    }

    public CalvalusShFileSystem(String username, JobClientsMap.CacheEntry cacheEntry) throws IOException {
        this.username = username;
        this.cacheEntry = cacheEntry;
        unixFileSystem = cacheEntry.getJobClientInternal().getFs();
        LOG.info("new CalvalusShFileSystem for " + username);
    }


    public String getUsername() {
        return username;
    }
    public JobClientsMap.CacheEntry getCacheEntry() {
        return cacheEntry;
    }
    public void setCacheEntry(JobClientsMap.CacheEntry cacheEntry) {
        this.cacheEntry = cacheEntry;
    }
    private void setAccessTime() {
        if (cacheEntry != null) {
            cacheEntry.setAccessTime();
        }
    }

    public FSDataInputStream open(Path f) throws IOException {
        return open(f, 4096);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        setAccessTime();
        String p = getUnixPath(path);
        Process proc = callUnixCommand("cat", p);
        LOG.info("file " + p + " externally opened for reading");
        return new DummyFSDataInputStream(proc.getInputStream());
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        setAccessTime();
        String p = getUnixPath(path);
        Process proc = callUnixCommand("ls -l", p);
        List<FileStatus> files = collectPathsOutput(proc);
        handleReturnCode(proc, files, path);
        LOG.info("dir " + path + " externally listed, " + files.size() + " entries");
        return files.toArray(new FileStatus[files.size()]);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        setAccessTime();
        String p = getUnixPath(path);
        if ("/".equals(p)) {
            LOG.info("file " + path + " externally listed, return default");
            return new FileStatus(1, true, 1, 1, 0, 0, FsPermission.createImmutable(Short.parseShort("0755", 8)), "cvop", "cvop", path);
        }
        //Process proc = callUnixCommand("stat -l", p);
        Process proc = callUnixCommand("stat -l", p);
        //List<FileStatus> files = collectPathsOutput(proc);
        List<FileStatus> files = collectPathsOutput(proc, p);
        handleReturnCode(proc, files, path);
        if (files.size() < 1) {
            LOG.info("file " + path + " externally listed, not found");
            throw new FileNotFoundException(path.toString());
        }
        LOG.info("file " + path + " externally listed, return entry");
        return files.get(0);
    }

    @Override
    public FileStatus[] globStatus(Path path) throws IOException {
        setAccessTime();
        String p = getUnixPath(path);
        Process proc = callUnixCommand("glob -l", p);
        List<FileStatus> files = collectPathsOutput(proc);
        handleReturnCode(proc, files, path);
        LOG.info("paths " + path + " externally listed, " + files.size() + " entries");
        for (int i=0; i<files.size(); ++i) {
            proc = callUnixCommand("stat -l", files.get(i).getPath().toString());
            List<FileStatus> files2 = collectPathsOutput(proc, files.get(i).getPath().toString());
            handleReturnCode(proc, files2, files.get(i).getPath());
            if (files2.size() > 0) {
                files.set(i, files2.get(0));
            }
        }
        return files.toArray(new FileStatus[files.size()]);
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        setAccessTime();
        String p = getUnixPath(path);
        Process proc = callUnixCommand("mkdirs", p, String.format("%o", permission.toShort()));
        LOG.info("dirs " + p + " externally created");
        return waitForReturnCode(proc, path) == 0;
    }

    private String getUnixPath(Path path) {
        String p = path.toUri().getPath();
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
                String path = isDir ? token[0].substring(0, token[0].length() - 1) : token[0];
                if (token.length >= 3) {
                    long length = Long.parseLong(token[1]);
                    long mtime = Long.parseLong(token[2]);
                    files.add(new FileStatus(length, isDir, 1, length, mtime, mtime, FsPermission.createImmutable(Short.parseShort("0777", 8)), "cvop", "cvop", new Path(path)));
                } else {
                    files.add(new FileStatus(1, isDir, 1, 1, 0, new Path(path)));
                }
            }
        }
        return files;
    }

    private List<FileStatus> collectPathsOutput(Process proc, String path) throws IOException {
        List<FileStatus> files = new ArrayList<FileStatus>();
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] token = line.split("\t");
                boolean isDir = token[0].endsWith("/");
                if (token.length >= 3) {
                    long length = Long.parseLong(token[1]);
                    long mtime = Long.parseLong(token[2]);
                    files.add(new FileStatus(length, isDir, 1, length, mtime, mtime, FsPermission.createImmutable(Short.parseShort("0777", 8)), "cvop", "cvop", new Path(path)));
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
        setAccessTime();
        String p = getUnixPath(path);
        Process proc = callUnixCommand("create", p, String.format("%o", permission.toShort()));
        LOG.info("file " + p + " externally opened for writing");
        return new FSDataOutputStream(proc.getOutputStream(), null);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        LOG.warning("rename not implemented, should rename " + src.toString() + " to " + dst.toString());
        return false;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        LOG.warning("delete not implemented, should delete " + f.toString());
        return false;
    }

    @Override
    public Configuration getConf() {
        return unixFileSystem.getConf();
    }
}
