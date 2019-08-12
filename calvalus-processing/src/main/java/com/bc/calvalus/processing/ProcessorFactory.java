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

import com.bc.calvalus.processing.beam.SnapGraphAdapter;
import com.bc.calvalus.processing.beam.SnapOperatorAdapter;
import com.bc.calvalus.processing.beam.SubsetProcessorAdapter;
import com.bc.calvalus.processing.executable.ExecutableProcessorAdapter;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.MapContext;
import org.esa.snap.core.datamodel.Product;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import static com.bc.calvalus.processing.hadoop.HadoopProcessingService.isArchive;

/**
 * Creates a {@code ProcessorAdapter} for the given processor.
 */
public class ProcessorFactory {

    public static final String CALVALUS_L2_PROCESSOR_FILES = "calvalus.l2.scriptFiles";
    private static final Logger logger = Logger.getLogger("com.bc.calvalus");

    enum ProcessorType {OPERATOR, GRAPH, EXEC, NONE}

    public static ProcessorAdapter createAdapter(MapContext mapContext) throws IOException {
        String processorTypeString = mapContext.getConfiguration().get(JobConfigNames.CALVALUS_L2_PROCESSOR_TYPE, "NONE");
        ProcessorType processorType = ProcessorType.valueOf(processorTypeString);
        switch (processorType) {
            case OPERATOR:
                return new SnapOperatorAdapter(mapContext);
            case GRAPH:
                return new SnapGraphAdapter(mapContext);
            case EXEC:
                return new ExecutableProcessorAdapter(mapContext);
            case NONE:
                return new SubsetProcessorAdapter(mapContext);

        }
        throw new IllegalArgumentException("Unknown processor type.");
    }

    public static void installProcessorBundles(String username, Configuration conf, FileSystem fs) throws IOException {
        ProcessorType processorType = ProcessorType.NONE;
        if (conf.get(JobConfigNames.CALVALUS_BUNDLES) != null) {
            final String[] aBundle = conf.get(JobConfigNames.CALVALUS_BUNDLES).split(",");
            List<String> processorFiles = new ArrayList<>();
            for (int i = 0; i < aBundle.length; i++) {
                final String bundleSpec = aBundle[i];
                Path bundlePath = getBundlePath(bundleSpec, conf, fs);
                if (bundlePath != null) {
//                    FileSystem fs;
//                    try {
//                        fs = FileSystem.get(bundlePath.toUri(), new JobConf(conf), conf.get(JobConfigNames.CALVALUS_USER));
//                    } catch (InterruptedException e) {
//                        throw new IOException(e);
//                    }
                    HadoopProcessingService.addBundleToClassPathStatic(bundlePath, conf, fs);
                    HadoopProcessingService.addBundleArchives(bundlePath, fs, conf);
                    HadoopProcessingService.addBundleLibs(bundlePath, fs, conf);

                    String executable = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR + "");
                    if (executable != null) {
                        // first bundle is the processor bundle
                        if (i == 0) {
                            processorType = detectProcessorType(bundlePath, executable, fs);
                        }
                        Collections.addAll(processorFiles, getBundleProcessorFiles(executable, bundlePath, fs));

                    }
                    // check for bundle to include, install it
                    try {
                        Path bundleDesc = new Path(bundlePath, HadoopProcessingService.BUNDLE_DESCRIPTOR_XML_FILENAME);
                        if (fs.exists(bundleDesc)) {
                            BundleDescriptor bundleDescriptor = HadoopProcessingService.readBundleDescriptor(fs, bundleDesc);
                            if (bundleDescriptor.getIncludeBundle() != null) {
                                Path includeBundlePath = new Path(bundlePath.getParent(), bundleDescriptor.getIncludeBundle());
                                HadoopProcessingService.addBundleToClassPathStatic(includeBundlePath, conf, fs);
                                HadoopProcessingService.addBundleArchives(includeBundlePath, fs, conf);
                                HadoopProcessingService.addBundleLibs(includeBundlePath, fs, conf);
                            }
                        }
                    } catch (Exception ex) {
                        logger.warning("reading bundle descriptor of " + bundlePath + " failed: " + ex);
                    }
                } else {
                    throw new IllegalArgumentException("Processor bundle " + bundleSpec + " does not exist.");
                }
            }
            if (processorFiles.size() > 0) {
                conf.setStrings(CALVALUS_L2_PROCESSOR_FILES + "", processorFiles.toArray(new String[processorFiles.size()]));
            }
        }

        conf.set(JobConfigNames.CALVALUS_L2_PROCESSOR_TYPE + "", processorType.toString());
    }

    private static ProcessorType detectProcessorType(Path bundlePath, final String executable, FileSystem fs) throws IOException {
        final FileStatus[] graphFiles = fs.listStatus(bundlePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                String filename = path.getName();
                return filename.equals(executable + "-graph.xml") || filename.equals(executable + "-graph.xml.vm");
            }
        });
        if (graphFiles.length == 1) {
            return ProcessorType.GRAPH;
        }
        final FileStatus[] executableFiles = fs.listStatus(bundlePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().equals(executable + "-process") ||
                       path.getName().equals(executable + "-process.vm");
            }
        });
        if (executableFiles.length == 1) {
            return ProcessorType.EXEC;
        }
        return ProcessorType.OPERATOR;
    }

    private static Path getBundlePath(String bundleSpec, Configuration conf, FileSystem fs) throws IOException {
        final Path bundlePath;
        if (isAbsolutePath(bundleSpec)) {
            bundlePath = new Path(bundleSpec);
        } else {
            bundlePath = new Path(HadoopProcessingService.CALVALUS_SOFTWARE_PATH, bundleSpec);
        }
//        FileSystem fs;
//        try {
//            fs = FileSystem.get(bundlePath.toUri(), new JobConf(conf), conf.get(JobConfigNames.CALVALUS_USER));
//        } catch (InterruptedException e) {
//            throw new IOException(e);
//        }
        if (fs.exists(bundlePath)) {
            FileStatus bundleStatus = fs.getFileStatus(bundlePath);
            if (bundleStatus != null && bundleStatus.isDirectory()) {
                return bundlePath;
            }
        }
        return null;
    }

    private static boolean isAbsolutePath(String bundleSpec) {
        return bundleSpec.charAt(0) == '/';
    }

/*
    private static void addBundleArchives(Path bundlePath, FileSystem fs, Configuration conf) throws IOException {
        final FileStatus[] archives = fs.listStatus(bundlePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return isArchive(path);
            }
        });
        for (FileStatus archive : archives) {
            URI uri = convertPathToURI(fs.makeQualified(archive.getPath()));
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

    private static void addBundleLibs(Path bundlePath, FileSystem fs, Configuration conf) throws IOException {
        final FileStatus[] libs = fs.listStatus(bundlePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return isLib(path);
            }
        });
        for (FileStatus lib : libs) {
            URI uri = fs.makeQualified(lib.getPath()).toUri();
            DistributedCache.addCacheFile(uri, conf);
        }
    }
*/

    private static String[] getBundleProcessorFiles(final String processorName, Path bundlePath, FileSystem fs) throws IOException {
        final FileStatus[] processorStatuses = fs.listStatus(bundlePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                String filename = path.getName();
                return (filename.startsWith("common-") || filename.startsWith(processorName + "-")) && !isArchive(path);
            }
        });
        String[] processorFiles = new String[processorStatuses.length];
        for (int i = 0; i < processorFiles.length; i++) {
            processorFiles[i] = processorStatuses[i].getPath().toString();
        }
        return processorFiles;
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
            processorAdapter.processSourceProduct(ProcessorAdapter.MODE.EXECUTE, ProgressMonitor.NULL);
            processorAdapter.saveProcessedProducts(ProgressMonitor.NULL);

            // MA only: use points from reference data set to restrict roi even further
            Product inputProduct = processorAdapter.getInputProduct();
            Rectangle referenceDataRoi = null; // calculated by client
            processorAdapter.setProcessingRectangle(referenceDataRoi);
            AffineTransform transform = processorAdapter.getInput2OutputTransform();

            Product product = processorAdapter.getProcessedProduct(ProgressMonitor.NULL);
            // do something with the processed product
        } finally {
            processorAdapter.dispose();
        }

    }
}
