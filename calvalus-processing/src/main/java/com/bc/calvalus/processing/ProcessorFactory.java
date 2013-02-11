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

import com.bc.calvalus.processing.beam.BeamProcessorAdapter;
import com.bc.calvalus.processing.beam.BeamProcessorInstaller;
import com.bc.calvalus.processing.beam.SubsetProcessorAdapter;
import com.bc.calvalus.processing.executable.ExecutableProcessorAdapter;
import com.bc.calvalus.processing.executable.ExecutableProcessorInstaller;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.MapContext;
import org.esa.beam.framework.datamodel.Product;

import java.awt.Rectangle;
import java.io.IOException;

/**
 * Creates a {@code ProcessorAdapter} for the given processor.
 */
public class ProcessorFactory {

    private static final String CALVALUS_L2_PROCESSOR_TYPE = "calvalus.l2.processorType";

    enum ProcessorType {BEAM, EXEC, NONE}

    public static ProcessorAdapter createAdapter(MapContext mapContext) throws IOException {
        String processorTypeString = mapContext.getConfiguration().get(CALVALUS_L2_PROCESSOR_TYPE, "NONE");
        ProcessorType processorType = ProcessorType.valueOf(processorTypeString);
        switch (processorType) {
            case BEAM:
                return new BeamProcessorAdapter(mapContext);
            case EXEC:
                return new ExecutableProcessorAdapter(mapContext);
            case NONE:
                return new SubsetProcessorAdapter(mapContext);

        }
        throw new IllegalArgumentException("Unknown processor type.");
    }

    public static void installProcessor(Configuration conf) throws IOException {
        ProcessorType processorType = ProcessorType.NONE;
        if (conf.get(JobConfigNames.CALVALUS_L2_BUNDLE) != null) {
            processorType = getProcessorType(conf);
            switch (processorType) {
                case BEAM:
                    new BeamProcessorInstaller(conf).install();
                    break;
                case EXEC:
                    new ExecutableProcessorInstaller(conf).install();
                    break;
            }
        }
        conf.set(CALVALUS_L2_PROCESSOR_TYPE, processorType.toString());
    }

    private static ProcessorType getProcessorType(Configuration conf) throws IOException {
        String bundle = conf.get(JobConfigNames.CALVALUS_L2_BUNDLE);
        final FileSystem fileSystem = FileSystem.get(conf);
        final Path bundlePath = new Path(HadoopProcessingService.CALVALUS_SOFTWARE_PATH, bundle);
        boolean bundleExist = doesBundleExists(bundlePath, fileSystem);
        if (!bundleExist) {
            throw new IllegalArgumentException("Processor bundle does not exist.");
        }
        final FileStatus[] fileStatuses = fileSystem.listStatus(bundlePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().endsWith(".jar");
            }
        });
        if (fileStatuses.length > 0) {
            return ProcessorType.BEAM;
        }
        return ProcessorType.EXEC;
    }

    public static boolean doesBundleExists(Path bundlePath, FileSystem fileSystem) throws IOException {
        if (fileSystem.exists(bundlePath))  {
            FileStatus bundleStatus = fileSystem.getFileStatus(bundlePath);
            return bundleStatus != null && bundleStatus.isDir();
        } else {
            return false;
        }
    }

    /**
     * This only for playing with the API, for understanding it.
     */
    public static void apiUsageExample(MapContext mapContext) throws Exception {
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
