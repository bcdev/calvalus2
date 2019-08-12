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

import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * A service for interacting with the filesystem.
 */
public interface FileSystemService {

    /**
     * Globs the given path pattern list against the inventory service's file system.
     * <p/>
     * <i>TODO: Use Unix file system wildcards instead (nf, 2011-09-09). See {@link DefaultInventoryService#getRegexpForPathGlob(String)}. </i>
     *
     * @param username     The user who will perform ths operation (to check rights).
     * @param pathPatterns A list of relative or absolute data paths which may contain regular expressions.
     * @return An array of fully qualified URIs comprising the filesystem and absolute data input path.
     * @throws java.io.IOException If an I/O error occurs
     */
    String[] globPaths(String username, List<String> pathPatterns) throws IOException;

    FileStatus[] globFiles(String username, List<String> pathPatterns) throws IOException;

    /**
     * @param username The user who will perform ths operation (to check rights).
     * @param path     A relative or absolute path.
     * @return A fully qualified URI comprising the filesystem and absolute data output path.
     */
    String getQualifiedPath(String username, String path) throws IOException;

    /**
     * Adds a file to the inventory (creates necessary directories) and returns an {@link OutputStream output stream} for writing data into it.
     *
     * @param username The user who will perform ths operation (to check rights).
     * @param path     the path to the file to be created.
     * @return the {@link OutputStream output stream} for writing.
     * @throws IOException If an I/O error occurs
     */
    OutputStream addFile(String username, String path) throws IOException;

    /**
     * Deletes the file specified by the given path.
     *
     * @param username The user who will perform ths operation (to check rights).
     * @param path     the path to the file to be deleted.
     * @return {@code true}, if the deletion was successful, otherwise {@code false}.
     * @throws IOException If an I/O error occurs
     */
    boolean removeFile(String username, String path) throws IOException;

    /**
     * Deletes the directory specified by the given path.
     *
     * @param username The user who will perform ths operation (to check rights).
     * @param path     the path to the directory to be deleted.
     * @return {@code true}, if the deletion was successful, otherwise {@code false}.
     * @throws IOException If an I/O error occurs
     */
    boolean removeDirectory(String username, String path) throws IOException;

    /**
     * Provides the information if the given path exists.
     *
     * @param path the path to check.
     * @param username the username to fetch the filesystem for. If <code>null</code>, the default username is used.
     * @return {@code true}, if the path exists, otherwise {@code false}.
     * @throws IOException If an I/O error occurs
     */
    boolean pathExists(String path, String username) throws IOException;

    /**
     * Opens the file specified by the given path for reading.
     *
     * @param username The user who will perform ths operation (to check rights).
     * @param path     the path to the file to be opened.
     * @return the {@link InputStream output stream} for reading.
     */
    InputStream openFile(String username, String path) throws IOException;
}
