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

package com.bc.calvalus.processing;

import com.bc.calvalus.processing.beam.BeamGraphAdapter;
import com.bc.calvalus.processing.beam.BeamOperatorAdapter;
import com.bc.calvalus.processing.beam.IdentityProcessorAdapter;
import com.bc.calvalus.processing.executable.ExecutableProcessorAdapter;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.MapContext;
import org.esa.beam.framework.datamodel.Product;

import java.awt.Rectangle;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Creates a {@code ProcessorAdapter} for the given processor.
 */
public class ProcessorFactory {

    public static final String CALVALUS_L2_PROCESSOR_FILES = "calvalus.l2.scriptFiles";
    private static final String CALVALUS_L2_PROCESSOR_TYPE = "calvalus.l2.processorType";

    enum ProcessorType {OPERATOR, GRAPH, EXEC, NONE}

    public static ProcessorAdapter createAdapter(MapContext mapContext) throws IOException {
        String processorTypeString = mapContext.getConfiguration().get(CALVALUS_L2_PROCESSOR_TYPE, "NONE");
        ProcessorType processorType = ProcessorType.valueOf(processorTypeString);
        switch (processorType) {
            case OPERATOR:
                return new BeamOperatorAdapter(mapContext);
            case GRAPH:
                return new BeamGraphAdapter(mapContext);
            case EXEC:
                return new ExecutableProcessorAdapter(mapContext);
            case NONE:
                return new IdentityProcessorAdapter(mapContext);

        }
        throw new IllegalArgumentException("Unknown processor type.");
    }

    public static void installProcessorBundle(Configuration conf) throws IOException {
        ProcessorType processorType = ProcessorType.NONE;
        String bundle = conf.get(JobConfigNames.CALVALUS_L2_BUNDLE);
        if (bundle != null) {
            final FileSystem fs = FileSystem.get(conf);
            Path bundlePath = getBundlePath(bundle, fs);
            if (bundlePath != null) {
                HadoopProcessingService.addBundleToClassPath(bundle, conf);
                addBundleArchives(bundlePath, fs, conf);
                DistributedCache.createSymlink(conf);

                String executable = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR);
                if (executable != null) {
                    processorType = detectProcessorType(bundlePath, executable, fs);
                    addBundleProcessorFiles(bundlePath, conf.get(JobConfigNames.CALVALUS_L2_OPERATOR), fs, conf);
                }
            } else {
                throw new IllegalArgumentException("Processor bundle does not exist.");
            }
        }
        conf.set(CALVALUS_L2_PROCESSOR_TYPE, processorType.toString());
    }

    private static void addBundleProcessorFiles(Path bundlePath, String processorName, FileSystem fs, Configuration conf) throws IOException {
        String[] processorFiles = getBundleProcessorFiles(processorName, bundlePath, fs);
        if (processorFiles.length > 0) {
            conf.setStrings(CALVALUS_L2_PROCESSOR_FILES, processorFiles);
        }
    }

    private static ProcessorType detectProcessorType(Path bundlePath, final String executable, FileSystem fs) throws IOException {
        final FileStatus[] graphFiles = fs.listStatus(bundlePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().equals(executable + "-graph.xml");
            }
        });
        if (graphFiles.length == 1) {
            return ProcessorType.GRAPH;
        }
        final FileStatus[] executableFiles = fs.listStatus(bundlePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().equals(executable + "-process") ||
                        path.getName().equals(executable + "-process.vm") ;
            }
        });
        if (executableFiles.length == 1) {
            return ProcessorType.EXEC;
        }
        return ProcessorType.OPERATOR;
    }

    static Path getBundlePath(String bundle, FileSystem fs) throws IOException {
        final Path bundlePath = new Path(HadoopProcessingService.CALVALUS_SOFTWARE_PATH, bundle);
        if (fs.exists(bundlePath)) {
            FileStatus bundleStatus = fs.getFileStatus(bundlePath);
            if (bundleStatus != null && bundleStatus.isDir()) {
                return bundlePath;
            }
        }
        return null;
    }

    private static void addBundleArchives(Path bundlePath, FileSystem fs, Configuration conf) throws IOException {
        final FileStatus[] archives = fs.listStatus(bundlePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return isArchive(path);
            }
        });
        for (FileStatus archive : archives) {
            URI uri = convertPathToURI(archive.getPath());
            DistributedCache.addCacheArchive(uri, conf);
        }
    }

    private static URI convertPathToURI(Path path) {
        URI uri = path.toUri();
        String linkName = stripArchiveExtension(path.getName());
        try {
            return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(), null, linkName);
        } catch (URISyntaxException ignore) {
            throw new IllegalArgumentException("could not add fragment to URI for Path: " + path);
        }
    }

    static String stripArchiveExtension(String archiveName) {
        if (archiveName.endsWith(".tgz") || archiveName.endsWith(".tar") || archiveName.endsWith(".zip")) {
            return archiveName.substring(0, archiveName.length() - 4);
        } else if (archiveName.endsWith(".tar.gz")) {
            return archiveName.substring(0, archiveName.length() - 7);
        }
        return null;
    }

    private static String[] getBundleProcessorFiles(final String processorName, Path bundlePath, FileSystem fs) throws IOException {
        final FileStatus[] processorStatuses = fs.listStatus(bundlePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(processorName + "-") && !isArchive(path);
            }
        });
        String[] processorFiles = new String[processorStatuses.length];
        for (int i = 0; i < processorFiles.length; i++) {
            processorFiles[i] = processorStatuses[i].getPath().toString();
        }
        return processorFiles;
    }

    /**
     * Hadoop can handle archives with the following extensions: zip, tar, tar.gz, tgz
     */
    static boolean isArchive(Path archivePath) {
        String filename = archivePath.getName();
        return filename.endsWith(".tgz") || filename.endsWith(".tar.gz") ||
                filename.endsWith(".tar") || filename.endsWith(".zip");
    }

    /**
     * This only for playing with the API of the ProcessorAdapter, for understanding it.
     */
    private static void apiUsageExample(MapContext mapContext) throws Exception {
        Configuration conf = mapContext.getConfiguration();
        ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(mapContext);
        try {
            // L2, including handing of processing of missing products
            processorAdapter.prepareProcessing();
            if (!conf.getBoolean(JobConfigNames.CALVALUS_PROCESS_ALL, false)) {
                if (processorAdapter.canSkipInputProduct()) {
                    // nothing to compute, result exists
                    return;
                }
            }
            processorAdapter.processSourceProduct(ProgressMonitor.NULL);
            processorAdapter.saveProcessedProducts(ProgressMonitor.NULL);

            // MA only: use points from reference data set to restrict roi even further
            Product inputProduct = processorAdapter.getInputProduct();
            Rectangle referenceDataRoi = null; // calculated by client
            processorAdapter.setProcessingRectangle(referenceDataRoi);

            Product product = processorAdapter.getProcessedProduct(ProgressMonitor.NULL);
            // do something with the processed product
        } finally {
            processorAdapter.dispose();
        }

    }
}
