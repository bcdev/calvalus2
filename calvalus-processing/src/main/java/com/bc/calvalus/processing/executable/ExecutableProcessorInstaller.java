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

package com.bc.calvalus.processing.executable;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.ProcessorInstaller;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Handles the installation of Executable processors onto cluster nodes.
 *
 * @author MarcoZ
 */
public class ExecutableProcessorInstaller implements ProcessorInstaller {

    private final Configuration configuration;

    public ExecutableProcessorInstaller(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void install() throws IOException {
        String bundle = configuration.get(JobConfigNames.CALVALUS_L2_BUNDLE);
        String executable = configuration.get(JobConfigNames.CALVALUS_L2_OPERATOR);
        if (bundle != null && executable != null) {
            final Path bundlePath = new Path(HadoopProcessingService.CALVALUS_SOFTWARE_PATH, bundle);
            final FileSystem fileSystem = FileSystem.get(configuration);

            if (!ProcessorFactory.doesBundleExists(bundlePath, fileSystem)) {
                throw new IllegalArgumentException("Path to bundle '" + bundlePath + "' does not exist.");
            }
            addBundleArchives(bundlePath, fileSystem);
            addBundleFiles(executable, bundlePath, fileSystem);

            DistributedCache.createSymlink(configuration);
        }
    }

    private void addBundleFiles(final String executable, Path bundlePath, FileSystem fileSystem) throws IOException {
        final FileStatus[] processorFiles = fileSystem.listStatus(bundlePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(executable + "-") && !isArchive(path);
            }
        });
        if (processorFiles.length == 0) {
            throw new IllegalArgumentException("Bundle contains no script files.");
        }
        String[] scriptFiles = new String[processorFiles.length];
        for (int i = 0; i < processorFiles.length; i++) {
            scriptFiles[i] = processorFiles[i].getPath().toString();
        }
        configuration.setStrings("calvalus.l2.scriptFiles", scriptFiles);
    }

    private void addBundleArchives(Path bundlePath, FileSystem fileSystem) throws IOException {
        final FileStatus[] archives = fileSystem.listStatus(bundlePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return isArchive(path);
            }
        });
        for (FileStatus archive : archives) {
            DistributedCache.addCacheArchive(convertPathToURI(archive.getPath()), configuration);
        }
    }

    private URI convertPathToURI(Path path) {
        URI uri = path.toUri();
        String linkName = stripArchiveExtension(path.getName());
        try {
            return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(), null, linkName);
        } catch (URISyntaxException ignore) {
            throw new IllegalArgumentException("could not add fragment to URI for Path: " + path);
        }
    }

    static String stripArchiveExtension(String archiveName) {
        if (archiveName.endsWith(".tgz") || archiveName.endsWith(".tar") || archiveName.endsWith(".zip") ) {
            return archiveName.substring(0, archiveName.length() - 4);
        } else if (archiveName.endsWith(".tar.gz")) {
            return archiveName.substring(0, archiveName.length() - 7);
        }
        return null;
    }

    /**
     * Hadoop can handle archives with the following extensions: zip, tar, tar.gz, tgz
     */
    static boolean isArchive(Path archivePath) {
        String filename = archivePath.getName();
        return filename.endsWith(".tgz") || filename.endsWith(".tar.gz") ||
                filename.endsWith(".tar") || filename.endsWith(".zip");
    }
}
