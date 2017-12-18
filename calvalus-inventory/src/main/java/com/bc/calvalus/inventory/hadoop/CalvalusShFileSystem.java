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
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemSetter;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.security.AccessControlException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A FileSystem that uses an external program to do ls and cat as user via setuid.
 *
 * @author boe
 */
public class CalvalusShFileSystem extends LocalFileSystem {

    public static final String CALVALUS_SH_COMMAND = "/usr/libexec/hadoop/calvalus-sh";
    private static final Logger LOG = CalvalusLogger.getLogger();

    protected final String username;
    protected JobClientsMap.CacheEntry cacheEntry = null;
    protected FileSystem unixFileSystem;
    protected boolean isLoginUser;

    public CalvalusShFileSystem() throws IOException {
        this.username = UserGroupInformation.getCurrentUser().getShortUserName();
        this.unixFileSystem = new LocalFileSystem();
        this.isLoginUser = username.equals(UserGroupInformation.getLoginUser().getShortUserName());
        LOG.info("new CalvalusShFileSystem for " + username + " " + isLoginUser);

    }

    public CalvalusShFileSystem(String username, FileSystem fs) throws IOException {
        this.username = username;
        this.unixFileSystem = fs;
        this.isLoginUser = username.equals(UserGroupInformation.getLoginUser().getShortUserName());
        LOG.info("new CalvalusShFileSystem for " + username + " " + isLoginUser);
    }

    public CalvalusShFileSystem(String username, JobClientsMap.CacheEntry cacheEntry) throws IOException {
        this.username = username;
        this.cacheEntry = cacheEntry;
        unixFileSystem = cacheEntry.getJobClientInternal().getFs();
        this.isLoginUser = username.equals(UserGroupInformation.getLoginUser().getShortUserName());
        LOG.info("new CalvalusShFileSystem for " + username + " " + isLoginUser);
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);
        unixFileSystem.initialize(name, conf);
        unixFileSystem.setConf(conf);
    }

    /** Maybe creates CalvalusShFileSystem and replaces user's file system in FileSystem registry, adds it to fileSystemMap */
    public static synchronized void createOrRegister(String username,
                                                     JobClientsMap.CacheEntry cacheEntry,
                                                     Map<String,FileSystem> fileSystemMap) throws IOException {
        FileSystem fs = cacheEntry.getJobClientInternal().getFs();
        if (fs instanceof CalvalusShFileSystem) {
            ((CalvalusShFileSystem) fs).setCacheEntry(cacheEntry);
            fileSystemMap.put(username, fs);
            LOG.info("file system with external access control cached for user " + username);
        } else if (username.equals(UserGroupInformation.getLoginUser().getShortUserName())) {
            LOG.info("avoid creation of CalvalusShFileSystem for login user " + username);
            fileSystemMap.put(username, fs);
        } else {
            final Configuration conf = cacheEntry.getJobClientInternal().getConf();
            final CalvalusShFileSystem cfs = new CalvalusShFileSystem(username, cacheEntry);
            cfs.initialize(fs.getUri(), conf);
            FileSystemSetter.addFileSystemForTesting(FileSystem.getDefaultUri(conf), conf, cfs);
            fileSystemMap.put(username, cfs);
            LOG.info("file system with external access control registered and cached for user " + username);
        }
    }

    /** Retrieves user's file system from FileSystem registry, maybe replaces it with new CalvalusShFileSystem */
    public static synchronized FileSystem getCreate(URI uri, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(uri, conf);
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        if (fs instanceof CalvalusShFileSystem) {
            LOG.info("file system with external access control pre-exists for " + ((CalvalusShFileSystem) fs).getUsername());
        } else if (currentUser == UserGroupInformation.getLoginUser()) {
            LOG.info("avoid registration of CalvalusShFileSystem for login user " + currentUser.getShortUserName());
        } else {
            final CalvalusShFileSystem cfs = new CalvalusShFileSystem(currentUser.getShortUserName(), fs);
            cfs.initialize(uri, conf);
            FileSystemSetter.addFileSystemForTesting(FileSystem.getDefaultUri(conf), conf, cfs);
            LOG.info("file system with external access control registered for user " + currentUser.getShortUserName());
            fs = cfs;
        }
        return fs;
    }


    private String getUsername() {
        return username;
    }

    public JobClientsMap.CacheEntry getCacheEntry() {
        return cacheEntry;
    }

    private void setCacheEntry(JobClientsMap.CacheEntry cacheEntry) {
        this.cacheEntry = cacheEntry;
    }

    private void setAccessTime() {
        if (cacheEntry != null) {
            cacheEntry.setAccessTime();
        }
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        setAccessTime();
        if (isLoginUser) { return unixFileSystem.listStatus(path); }
        String p = path.toUri().getPath();
        Process proc = callUnixCommand("ls", p);
        List<FileStatus> files = collectPathsOutput(proc);
        handleReturnCode(proc, files, path);
        LOG.info("dir " + path + " externally listed, " + files.size() + " entries");
        return files.toArray(new FileStatus[files.size()]);
    }

    @Override
    public FileStatus[] listStatus(Path path, PathFilter filter) throws FileNotFoundException, IOException {
        setAccessTime();
        if (isLoginUser) { return unixFileSystem.listStatus(path, filter); }
        String p = path.toUri().getPath();
        Process proc = callUnixCommand("ls", p);
        List<FileStatus> files = collectPathsOutput(proc);
        handleReturnCode(proc, files, path);
        for (int i = 0; i < files.size();) {
            if (! filter.accept(files.get(i).getPath())) {
                files.remove(i);
            } else {
                ++i;
            }
        }
        LOG.info("dir " + path + " externally listed and filtered, " + files.size() + " entries");
        return files.toArray(new FileStatus[files.size()]);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
    throws IOException {
      return new RemoteIterator<LocatedFileStatus>() {
        private final FileStatus[] stats = listStatus(f);
        private int i = 0;

        @Override
        public boolean hasNext() {
          return i<stats.length;
        }

        @Override
        public LocatedFileStatus next() throws IOException {
          if (!hasNext()) {
            throw new NoSuchElementException("No more entry in " + f);
          }
          FileStatus result = stats[i++];
          // for files, use getBlockLocations(FileStatus, int, int) to avoid
          // calling getFileStatus(Path) to load the FileStatus again
          BlockLocation[] locs = result.isFile() ?
              getFileBlockLocations(result, 0, result.getLen()) :
              null;
          return new LocatedFileStatus(result, locs);
        }
      };
    }


    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        setAccessTime();
        if (isLoginUser) { return unixFileSystem.getFileStatus(path); }
        String p = path.toUri().getPath();
        if ("/".equals(p)) {
            LOG.info("file " + p + " externally listed, return default");
            return new FileStatus(1, true, 1, 1, 0, 0, FsPermission.createImmutable(Short.parseShort("0755", 8)), "cvop", "cvop", path);
        }
        Process proc = callUnixCommand("stat", p);
        List<FileStatus> files = collectPathsOutput(proc);
        handleReturnCode(proc, files, path);
        if (files.size() < 1) {
            LOG.setLevel(Level.INFO);
            LOG.info("file " + p + " externally listed for user " + username + ", not found");
            throw new FileNotFoundException(p);
        }
        LOG.info("file " + p + " externally listed, return entry");
        return files.get(0);
    }

    @Override
    public FileStatus[] globStatus(Path path) throws IOException {
        setAccessTime();
        if (isLoginUser) { return unixFileSystem.globStatus(path); }
        String p = path.toUri().getPath();
        Process proc = callUnixCommand("glob", p);
        List<FileStatus> files = collectPathsOutput(proc);
        handleReturnCode(proc, files, path);
        LOG.info("paths " + p + " externally listed, " + files.size() + " entries");
        return files.toArray(new FileStatus[files.size()]);
    }

    @Override
    public FileStatus[] globStatus(Path path, PathFilter filter) throws IOException {
        setAccessTime();
        if (isLoginUser) { return unixFileSystem.globStatus(path); }
        String p = path.toUri().getPath();
        Process proc = callUnixCommand("glob", p);
        List<FileStatus> files = collectPathsOutput(proc);
        handleReturnCode(proc, files, path);
        for (int i = 0; i < files.size();) {
            if (! filter.accept(files.get(i).getPath())) {
                files.remove(i);
            } else {
                ++i;
            }
        }
        LOG.info("paths " + p + " externally listed and filtered, " + files.size() + " entries");
        return files.toArray(new FileStatus[files.size()]);
    }

    @Override
    public FSDataInputStream open(Path path) throws IOException {
        if (isLoginUser) { return unixFileSystem.open(path); }
        return open(path, 4096);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        setAccessTime();
        if (isLoginUser) { return unixFileSystem.open(path); }
        final String p = path.toUri().getPath();
        Process proc = callUnixCommand("cat", p);
        LOG.info("file " + p + " externally opened for reading");
        return new DummyFSDataInputStream(proc.getInputStream()){
            @Override
            public void close() throws IOException {
                super.close();
                LOG.info("file " + p + " externally closed");
            }
        };
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        setAccessTime();
        if (isLoginUser) { return unixFileSystem.create(path, permission, overwrite, bufferSize, replication, blockSize, progress); }
        final String p = path.toUri().getPath();
        if (! exists(path.getParent())) {
            mkdirs(path.getParent(), FsPermission.createImmutable(Short.parseShort("0777", 8)));
        }
        Process proc = callUnixCommand("create", p, String.format("%o", permission.toShort()));
        LOG.info("file " + p + " externally opened for writing");
        return new FSDataOutputStream(proc.getOutputStream(), null) {
            @Override
            public void close() throws IOException {
                super.close();
                LOG.info("file " + p + " externally closed");
            }
        };
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
        setAccessTime();
        if (isLoginUser) { return unixFileSystem.append(path, bufferSize, progress); }
        if (!exists(path)) {
            throw new FileNotFoundException("File " + path + " not found");
        }
        final String p = path.toUri().getPath();
        Process proc = callUnixCommand("append", p);
        LOG.info("file " + p + " externally opened for appending");
        return new FSDataOutputStream(proc.getOutputStream(), null){
            @Override
            public void close() throws IOException {
                super.close();
                LOG.info("file " + p + " externally closed");
            }
        };
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        setAccessTime();
        if (isLoginUser) { return unixFileSystem.mkdirs(path, permission); }
        String p = path.toUri().getPath();
        Process proc = callUnixCommand("mkdirs", p, String.format("%o", permission.toShort()));
        LOG.info("dirs " + p + " externally created");
        return waitForReturnCode(proc, path) == 0;
    }

    @Override
    public void setPermission(Path path, FsPermission permission) throws IOException {
        setAccessTime();
        if (isLoginUser) { unixFileSystem.setPermission(path, permission); return; }
        String p = path.toUri().getPath();
        Process proc = callUnixCommand("chmod", p, String.format("%o", permission.toShort()));
        LOG.info("permission of " + p + " externally set");
        waitForReturnCode(proc, path);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        setAccessTime();
        if (isLoginUser) { return unixFileSystem.rename(src, dst); }
        String p1 = src.toUri().getPath();
        String p2 = dst.toUri().getPath();
        Process proc = callUnixCommand("mv", p1, p2);
        LOG.info("path " + p1 + " externally renamed to " + p2);
        return waitForReturnCode(proc, src) == 0;
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        setAccessTime();
        if (isLoginUser) { return unixFileSystem.delete(path, recursive); }
        String p = path.toUri().getPath();
        Process proc = callUnixCommand(recursive ? "rm -r" : "rm", p);
        LOG.info("path " + p + " deleted");
        return waitForReturnCode(proc, path) == 0;
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
    public URI getUri() {
        return unixFileSystem.getUri();
    }

    @Override
    public Path makeQualified(Path path) {
        Path p = unixFileSystem.makeQualified(path);
        //LOG.fine("makeQualified of " + path + " is " + p.toUri() + " scheme " + p.toUri().getScheme());
        return p;
    }

    @Override
    public Configuration getConf() {
        return unixFileSystem.getConf();
    }


    private Process callUnixCommand(String cmd, String... path) throws IOException {
        ProcessBuilder pb = path.length == 1
                ? new ProcessBuilder(CALVALUS_SH_COMMAND, username, cmd, path[0])
                : new ProcessBuilder(CALVALUS_SH_COMMAND, username, cmd, path[0], path[1]);
        pb.redirectErrorStream(true);
        Process proc = pb.start();
        if (path.length == 1) {
            LOG.info("calling " + CALVALUS_SH_COMMAND + " " + username + " " + cmd + " " + path[0]);
        } else {
            LOG.info("calling " + CALVALUS_SH_COMMAND + " " + username + " " + cmd + " " + path[0] + " " + path[1]);
        }
        return proc;
    }

    private List<FileStatus> collectPathsOutput(Process proc) throws IOException {
        List<FileStatus> files = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] token = line.split("\t");
                boolean isDir = token[0].endsWith("/");
                Path path = new Path("file://" + (isDir ? token[0].substring(0, token[0].length() - 1) : token[0]));
                // /hd1/yarn/local/usercache/      4096    1507550699000   755     yarn    hadoop
                if (token.length >= 6) {
                    long length = Long.parseLong(token[1]);
                    long mtime = Long.parseLong(token[2]);
                    FsPermission perm = FsPermission.createImmutable(Short.parseShort(token[3], 8));
                    files.add(new FileStatus(length, isDir, 1, length, mtime, mtime, perm, token[4], token[5], path));
                } else if (token.length >= 3) {
                    long length = Long.parseLong(token[1]);
                    long mtime = Long.parseLong(token[2]);
                    files.add(new FileStatus(length, isDir, 1, length, mtime, mtime, FsPermission.createImmutable(Short.parseShort("0777", 8)), "cvop", "cvop", path));
                } else {
                    files.add(new FileStatus(1, isDir, 1, 1, 0, path));
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
        } catch (InterruptedException _e) {
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
        } catch (InterruptedException _e) {
            LOG.fine("path " + path.toString() + " interrupted");
            return 1;
        }

    }
}
