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

package com.bc.calvalus.inventory;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.inventory.hadoop.FileSystemPathIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract implementation of the {@link FileSystemService}.
 *
 * @author MarcoZ
 * @author Norman
 * @author Martin (for ACL)
 */
public abstract class AbstractFileSystemService implements FileSystemService {

    private final JobClientsMap jobClientsMap;

    public AbstractFileSystemService(JobClientsMap jobClientsMap) {
        this.jobClientsMap = jobClientsMap;
    }

    /**
     * @return An absolute path that is used to make relative paths absolute.
     */
    protected abstract String getContextPath();

    @Override
    public String[] globPaths(String username, List<String> pathPatterns) throws IOException {
        Configuration conf = jobClientsMap.getConfiguration();
        Pattern pattern = createPattern(pathPatterns, conf);
        String commonPathPrefix = getCommonPathPrefix(pathPatterns);
        FileSystem fileSystem = jobClientsMap.getFileSystem(username, commonPathPrefix);
        Path qualifiedPath = makeQualified(fileSystem, commonPathPrefix);
        List<FileStatus> fileStatuses = new ArrayList<>(1000);
        collectFileStatuses(fileSystem, qualifiedPath, pattern, fileStatuses);
        String[] result = new String[fileStatuses.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = fileStatuses.get(i).getPath().toString();
        }
        return result;
    }

    @Override
    public FileStatus[] globFiles(String username, List<String> pathPatterns) throws IOException {
        Configuration conf = jobClientsMap.getConfiguration();
        Pattern pattern = createPattern(pathPatterns, conf);
        String commonPathPrefix = getCommonPathPrefix(pathPatterns);
        FileSystem fileSystem = jobClientsMap.getFileSystem(username, commonPathPrefix);
        Path qualifiedPath = makeQualified(fileSystem, commonPathPrefix);
        List<FileStatus> fileStatuses = new ArrayList<>(1000);
        collectFileStatuses(fileSystem, qualifiedPath, pattern, fileStatuses);
        return fileStatuses.toArray(new FileStatus[0]);
    }

    @Override
    public String getQualifiedPath(String username, String path) throws IOException {
        FileSystem fileSystem = jobClientsMap.getFileSystem(username, path);
        Path qualifiedPath = makeQualified(fileSystem, path);
        return qualifiedPath.toString();
    }

    @Override
    public OutputStream addFile(String username, String path) throws IOException {
        FileSystem fileSystem = jobClientsMap.getFileSystem(username, path);
        Path qualifiedPath = makeQualified(fileSystem, path);
        return fileSystem.create(qualifiedPath);
    }

    @Override
    public boolean removeFile(String username, String path) throws IOException {
        FileSystem fileSystem = jobClientsMap.getFileSystem(username, path);
        Path qualifiedPath = makeQualified(fileSystem, path);
        return fileSystem.delete(qualifiedPath, false);
    }

    @Override
    public boolean removeDirectory(String username, String path) throws IOException {
        FileSystem fileSystem = jobClientsMap.getFileSystem(username, path);
        Path qualifiedPath = makeQualified(fileSystem, path);
        if (fileSystem.exists(qualifiedPath)) {
            return fileSystem.delete(qualifiedPath, true);
        } else {
            return false;
        }
    }

    @Override
    public boolean pathExists(String path, String username) throws IOException {
        Configuration conf = jobClientsMap.getConfiguration();

        FileSystem fileSystem;
        if (username != null) {
            try {
                fileSystem = FileSystem.get(new Path(path).toUri(), conf, username);
            } catch (InterruptedException e) {
                throw new RuntimeException("Unable to access file system.", e);
            }
        } else {
            fileSystem = new Path(path).getFileSystem(conf);
        }

        Path qualifiedPath = makeQualified(fileSystem, path);
        return fileSystem.exists(qualifiedPath);
    }

    @Override
    public InputStream openFile(String username, String path) throws IOException {
        FileSystem fileSystem = jobClientsMap.getFileSystem(username, path);
        Path qualifiedPath = makeQualified(fileSystem, path);
        return fileSystem.open(qualifiedPath);
    }

    // use globFileStatusIterator instead 
    @Deprecated
    public FileStatus[] globFileStatuses(List<String> pathPatterns, Configuration conf) throws IOException {
        Pattern pattern = createPattern(pathPatterns, conf);
        String commonPathPrefix = getCommonPathPrefix(pathPatterns);
        FileSystem commonFS = new Path(commonPathPrefix).getFileSystem(conf);
        Path qualifiedPath = makeQualified(commonFS, commonPathPrefix);
        return collectFileStatuses(commonFS, qualifiedPath, pattern);
    }

    public RemoteIterator<LocatedFileStatus> globFileStatusIterator(List<String> pathPatterns, Configuration conf, FileSystemPathIterator.FileStatusFilter extraFilter) throws IOException {
        Pattern pattern = createPattern(pathPatterns, conf);
        String commonPathPrefix = getCommonPathPrefix(pathPatterns);
        FileSystem fs = new Path(commonPathPrefix).getFileSystem(conf);
        Path rootPath = makeQualified(fs, commonPathPrefix);
        List<FileSystemPathIterator.FileStatusFilter> acceptFilter = new ArrayList<>();
        acceptFilter.add(FileSystemPathIterator.HIDDEN_FILTER);
        if (pattern != null) {
            acceptFilter.add(FileSystemPathIterator.filterPattern(pattern));
        }
        if (extraFilter != null) {
            acceptFilter.add(extraFilter);
        }
        return new FileSystemPathIterator(fs, acceptFilter).listFiles(rootPath, true);
    }
    
    public Path makeQualified(FileSystem fileSystem, String child) {
        Path path = new Path(child);
        if (!path.isAbsolute()) {
            path = new Path(getContextPath(), path);
        }
        return fileSystem.makeQualified(path);
    }

    public FileSystem getFileSystem(String username) throws IOException {
        return jobClientsMap.getFileSystem(username);
    }

    private Pattern createPattern(List<String> inputRegexs, Configuration conf) throws IOException {
        if (inputRegexs.size() == 0) {
            return null;
        }
        StringBuilder hugePattern = new StringBuilder(inputRegexs.size() * inputRegexs.get(0).length());
        for (String regex : inputRegexs) {
            FileSystem fileSystem = new Path(regex).getFileSystem(conf);
            Path qualifiedPath = makeQualified(fileSystem, regex);
            hugePattern.append(qualifiedPath.toUri().getPath().toString());
            hugePattern.append("|");
        }
        hugePattern.setLength(hugePattern.length() - 1);
        return Pattern.compile(hugePattern.toString());
    }

    private FileStatus[] collectFileStatuses(FileSystem fileSystem, Path path, Pattern pattern) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>(1000);
        collectFileStatuses(fileSystem, path, pattern, result);
        return result.toArray(new FileStatus[result.size()]);
    }

    private void collectFileStatuses(FileSystem fileSystem, Path path, Pattern pattern, List<FileStatus> result) throws IOException {
        if (!fileSystem.exists(path)) {
            return;
        }
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        if (fileStatuses == null) {
            return;
        }
        Matcher matcher = null;
        if (pattern != null) {
            matcher = pattern.matcher("");
        }
        for (FileStatus fStat : fileStatuses) {
            try {
                String filename = fStat.getPath().getName();
                if (!filename.startsWith("_") && !filename.startsWith(".")) {
                    if (fStat.isDirectory()) {
                        collectFileStatuses(fileSystem, fStat.getPath(), pattern, result);
                    } else {
                        String fPath = fStat.getPath().toUri().getPath();
                        if (matcher != null) {
                            matcher.reset(fPath);
                            if (matcher.matches()) {
                                result.add(fStat);
                            }
                        } else {
                            result.add(fStat);
                        }
                    }
                }
            } catch (AccessControlException ignore) {}
        }
    }

    static String getCommonPathPrefix(List<String> strings) {
        if (strings.size() == 0) {
            return "";
        }
        char firstChar = 0;
        for (int pos = 0; ; pos++) {
            for (int i = 0; i < strings.size(); i++) {
                String string = strings.get(i);
                if (pos == string.length()) {
                    return string;
                }
                if (string.charAt(pos) == '*') {
                    return stripAfterLastSlash(string, pos);
                }
                if (i == 0) {
                    firstChar = string.charAt(pos);
                } else if (string.charAt(pos) != firstChar) {
                    return stripAfterLastSlash(string, pos);
                }
            }
        }
    }

    private static String stripAfterLastSlash(String string, int pos) {
        int slashPos = string.lastIndexOf('/', pos);
        if (slashPos != -1) {
            return string.substring(0, slashPos);
        } else {
            return "";
        }
    }

    public static String getUserGlob(String userName, String dirPath) {
        return getUserPath(userName, dirPath) + "/.*";
    }

    public static String getUserPath(String userName, String dirPath) {
        String path;
        if (dirPath.isEmpty() || "/".equals(dirPath)) {
            path = String.format("home/%s", userName.toLowerCase());
        } else {
            path = String.format("home/%s/%s", userName.toLowerCase(), dirPath);
        }
        return path;
    }

    public JobClientsMap getJobClientsMap() {
        return jobClientsMap;
    }
}
