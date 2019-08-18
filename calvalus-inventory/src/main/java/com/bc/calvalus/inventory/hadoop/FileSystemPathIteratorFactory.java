/*
 * Copyright (C) 2017 Brockmann Consult GmbH (info@brockmann-consult.de) 
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

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.AccessControlException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Traverse a Hadoop file system tree, code and ideas taken from:
 * <p>
 * org.apache.hadoop.fs.FileSystem.listFiles(Path p, boolean recursive)
 * org.apache.hadoop.fs.FileSystem.listLocatedStatus(Path p, PathFilter pf)
 */
public class FileSystemPathIteratorFactory {

    public static final FileStatusFilter HIDDEN_FILTER = new HiddenFileStatusFilter();

    private final FileSystem fs;
    private final FileStatusFilter filter;

    public FileSystemPathIteratorFactory(FileSystem fs, List<FileStatusFilter> filter) {
        this(fs, filter.toArray(new FileStatusFilter[0]));
    }

    public FileSystemPathIteratorFactory(FileSystem fs, FileStatusFilter... filter) {
        this.fs = fs;
        this.filter = new CombinedFileStatusFilter(filter);
    }

    private RemoteIterator<LocatedFileStatus> applyFilter(RemoteIterator<LocatedFileStatus> fileStatusIt) throws IOException {
        return new RemoteIterator<LocatedFileStatus>() {
            LocatedFileStatus next = getNext();
            @Override
            public boolean hasNext() throws IOException {
                return next != null;
            }
            @Override
            public LocatedFileStatus next() throws IOException {
                LocatedFileStatus current = next;
                next = getNext();
                return current;
            }
            private LocatedFileStatus getNext() throws IOException {
                while (fileStatusIt.hasNext()) {
                    LocatedFileStatus candidate = fileStatusIt.next();
                    if (filter.accept(candidate)) {
                        return candidate;
                    }
                }
                return null;
            }
        };
    }


    /**
     * List the statuses and block locations of the files in the given path.
     * Does not guarantee to return the iterator that traverses statuses
     * of the files in a sorted order.
     * <p>
     * <pre>
     * If the path is a directory,
     *   if recursive is false, returns files in the directory;
     *   if recursive is true, return files in the subtree rooted at the path.
     * If the path is a file, return the file's status and block locations.
     * </pre>
     *
     * @param f         is the path
     * @param recursive if the subdirectories need to be traversed recursively
     * @return an iterator that traverses statuses of the files
     * @throws IOException see specific implementation
     */
    public RemoteIterator<LocatedFileStatus> listFiles(final Path f, final boolean recursive, boolean withDirs, int maxDepth) throws IOException {
        try {
            return applyFilter(new LocatedFileStatusRemoteIterator(f, recursive, withDirs, maxDepth));
        } catch (FileNotFoundException fnfe) {
            return new EmptyRemoteIterator<>();
        }
    }

    /**
     * List a directory.
     * The returned results include its block location if it is a file
     *
     * @param f a path
     * @return an iterator that traverses statuses of the files/directories in the given path
     * @throws FileNotFoundException if <code>f</code> does not exist
     * @throws IOException           if any I/O error occurred
     */
    private RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws FileNotFoundException, IOException {
        return new RemoteIterator<LocatedFileStatus>() {
            private final FileStatus[] stats = listStatus(f);
            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < stats.length;
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more entries in " + f);
                }
                FileStatus result = stats[i++];
                // for files, use getBlockLocations(FileStatus, int, int) to avoid
                // calling getFileStatus(Path) to load the FileStatus again
                BlockLocation[] locs = null;
                if (result.isFile()) {
                    try {
                        locs = fs.getFileBlockLocations(result, 0, result.getLen());
                    } catch (AccessControlException ignore) {
                    }
                }
                return new LocatedFileStatus(result, locs);
            }
        };
    }

    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        FileStatus[] listing;
        try {
            listing = fs.listStatus(f);
            if (listing == null) {
                return new FileStatus[0];
            }
        } catch (AccessControlException ignore) {
            return new FileStatus[0];
        }
        // TODO: seems that this applies the filter to directories too early
        //return Arrays.stream(listing).filter(filter::accept).toArray(FileStatus[]::new);
        return listing;
    }


    private class LocatedFileStatusRemoteIterator implements RemoteIterator<LocatedFileStatus> {

        private final Stack<RemoteIterator<LocatedFileStatus>> itors;
        private final Path rootPath;
        private final boolean recursive;
        private boolean withDirs;
        private RemoteIterator<LocatedFileStatus> curItor;
        private LocatedFileStatus curFile;
        private int depth;

        LocatedFileStatusRemoteIterator(Path rootPath, boolean recursive, boolean withDirs, int maxDepth) throws IOException {
            this.rootPath = rootPath;
            this.recursive = recursive;
            this.withDirs = withDirs;
            itors = new Stack<>();
            if (withDirs) {
                curFile = new LocatedFileStatus(new FileStatus(0, true, 0, 0, 0, rootPath), null);
            }
            if (maxDepth > 0) {
                curItor = listLocatedStatus(rootPath);
            } else {
                curItor = new RemoteIterator<LocatedFileStatus>() {
                    @Override
                    public boolean hasNext() throws IOException {
                        return false;
                    }
                    @Override
                    public LocatedFileStatus next() throws IOException {
                        return null;
                    }
                };
            }
            this.depth = maxDepth - 1;
        }

        @Override
        public boolean hasNext() throws IOException {
            while (curFile == null) {
                if (curItor.hasNext()) {
                    handleFileStat(curItor.next());
                } else if (!itors.empty()) {
                    curItor = itors.pop();
                    ++depth;
                } else {
                    return false;
                }
            }
            return true;
        }

        /**
         * Process the input stat.
         * If it is a file, return the file stat.
         * If it is a directory, traverse the directory if recursive is true;
         * ignore it if recursive is false.
         *
         * @param stat input status
         * @throws IOException if any IO error occurs
         */
        private void handleFileStat(LocatedFileStatus stat) throws IOException {
            if (stat.isFile()) {
                curFile = stat;
            } else {
                if (withDirs) {
                    curFile = stat;
                }
                if (recursive && depth > 0) { // directory
                    itors.push(curItor);
                    --depth;
                    curItor = listLocatedStatus(stat.getPath());
                }
            }
        }

        @Override
        public LocatedFileStatus next() throws IOException {
            if (hasNext()) {
                LocatedFileStatus result = curFile;
                curFile = null;
                return result;
            }
            throw new NoSuchElementException("No more entry in " + rootPath);
        }
    }

    public static FileStatusFilter filterPattern(Pattern pattern, boolean withDirs) {
        return new PatternFileStatusFilter(pattern, withDirs);
    }

    public interface FileStatusFilter {
        boolean accept(FileStatus fileStatus);
    }

    private static class CombinedFileStatusFilter implements FileStatusFilter {

        private FileStatusFilter[] filters;

        public CombinedFileStatusFilter(FileStatusFilter... filters) {
            this.filters = filters;
        }

        public boolean accept(FileStatus fileStatus) {
            for (FileStatusFilter filter : filters) {
                if (!filter.accept(fileStatus)) {
                    return false;
                }
            }
            return true;
        }
    }

    private static class HiddenFileStatusFilter implements FileStatusFilter {

        @Override
        public boolean accept(FileStatus fileStatus) {
            String filename = fileStatus.getPath().getName();
            return !filename.startsWith("_") && !filename.startsWith(".");
        }
    }

    private static class PatternFileStatusFilter implements FileStatusFilter {
        private final Matcher matcher;
        private boolean withDirs;

        public PatternFileStatusFilter(Pattern pattern, boolean withDirs) {
            matcher = pattern.matcher("");
            this.withDirs = withDirs;
        }

        @Override
        public boolean accept(FileStatus fileStatus) {
            if (fileStatus.isFile() || withDirs) {
                String fPath = fileStatus.getPath().toUri().getPath();
                matcher.reset(fPath);
                return matcher.matches();
            }
            return true;
        }
    }

    private static class EmptyRemoteIterator<E> implements RemoteIterator<E> {
        public boolean hasNext() {
            return false;
        }

        public E next() {
            throw new NoSuchElementException();
        }
    }
}
