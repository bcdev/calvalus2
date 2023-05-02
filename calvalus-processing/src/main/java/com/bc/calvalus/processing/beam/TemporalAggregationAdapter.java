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

package com.bc.calvalus.processing.beam;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;

import java.io.File;
import java.io.IOException;

import static org.esa.snap.core.util.io.FileUtils.exchangeExtension;

/**
 * A processor adapter that uses a SNAP GPF {@code Operator} to process an input product.
 *
 * @author MarcoZ
 */
public class TemporalAggregationAdapter extends SubsetProcessorAdapter {

    private TemporalAggregator aggregator;

    public TemporalAggregationAdapter(MapContext mapContext) {
        super(mapContext);
        try {
            String aggregatorName = mapContext.getConfiguration().get(JobConfigNames.CALVALUS_L2_PROCESSOR_IMPL);
            getLogger().info("creating aggregator " + aggregatorName);
            aggregator = (TemporalAggregator) Class.forName(aggregatorName).newInstance();
        } catch (ClassNotFoundException  | InstantiationException  | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean processSourceProduct(MODE mode, ProgressMonitor pm) throws IOException {
        pm.setSubTaskName("temporal aggregation");
        Path inputPath = getInputPath();
        File inputFile = getInputFile();
        Path[] additionalInputPaths = getAdditionalInputPaths();
        File inputData;

        try {
            // retrieve first input, unpack, if zipped, TODO handle tar as well
            if (inputFile == null) {
                if (inputPath.getName().endsWith(".zip")) {
                    for (File entry : CalvalusProductIO.uncompressArchiveToCWD(inputPath, getConfiguration())) {
                        if ("xfdumanifest.xml".equals(entry.getName()) || entry.getName().endsWith("MTD.xml") || entry.getName().endsWith(".dim")) {
                            inputFile = entry;
                            break;
                        }
                    }
                    if (inputFile == null) {
                        inputFile = new File(inputPath.getName().substring(0, inputPath.getName().length()-4));
                    }
                    inputData = new File(inputPath.getName().substring(0, inputPath.getName().length()-4));
                } else {
                    inputFile = CalvalusProductIO.copyFileToLocal(inputPath, getConfiguration());
                    inputData = new File(inputPath.getName());
                }
                setInputFile(inputFile);
            } else {
                inputData = inputFile;
            }

            // initialise aggregator and aggregate first input
            final Product firstInputProduct = getInputProduct();
            Product inputProduct;
            if (getConfiguration().getBoolean(JobConfigNames.CALVALUS_INPUT_SUBSETTING, true)) {
                inputProduct = createSubsetFromInput(firstInputProduct);
            } else {
                inputProduct = firstInputProduct;
            }
            aggregator.initialize(getMapContext().getConfiguration(), inputProduct);
            LOG.info("aggregating 1/" + (additionalInputPaths.length + 1) + " " + inputFile.getName());
            aggregator.aggregate(inputProduct);
            ProductData.UTC startTime = firstInputProduct.getStartTime();
            ProductData.UTC stopTime = firstInputProduct.getEndTime();
            LOG.info("closing " + inputData.getName());
            if (inputProduct != firstInputProduct) {
                firstInputProduct.dispose();
            }
            dispose();
            if (inputData.isDirectory()) {
                FileUtils.deleteDirectory(inputData);
            } else {
                inputData.delete();
            }

            // aggregate the time series one by one
            if (additionalInputPaths != null) {
                int count = 1;
                for (Path additionalInputPath : additionalInputPaths) {
                    if (additionalInputPath.getFileSystem(getConfiguration()).exists(additionalInputPath)) {
                        // staging input
                        if (additionalInputPath.getName().endsWith(".zip")) {
                            for (File entry : CalvalusProductIO.uncompressArchiveToCWD(additionalInputPath, getConfiguration())) {
                                if ("xfdumanifest.xml".equals(entry.getName()) || entry.getName().endsWith("MTD.xml") || entry.getName().endsWith(".dim")) {
                                    inputFile = entry;
                                    break;
                                }
                            }
                            if (inputFile == null) {
                                inputFile = new File(additionalInputPath.getName().substring(0, additionalInputPath.getName().length()-4));
                            }
                            inputData = new File(additionalInputPath.getName().substring(0, additionalInputPath.getName().length()-4));
                        } else {
                            inputFile = CalvalusProductIO.copyFileToLocal(additionalInputPath, getConfiguration());
                            inputData = new File(additionalInputPath.getName());
                        }
                        // opening input, create subset if requested
                        setInputFile(inputFile);
                        final Product nextInputProduct = getInputProduct();
                        if (getConfiguration().getBoolean(JobConfigNames.CALVALUS_INPUT_SUBSETTING, true)) {
                            inputProduct = createSubsetFromInput(nextInputProduct);
                        } else {
                            inputProduct = nextInputProduct;
                        }
                        // aggregating input
                        LOG.info("aggregating " + (++count) + "/" + (additionalInputPaths.length + 1) + " " + inputFile.getName());
                        aggregator.aggregate(inputProduct);
                        if (nextInputProduct.getStartTime().getMJD() < startTime.getMJD()) {
                            startTime = nextInputProduct.getStartTime();
                        }
                        if (nextInputProduct.getEndTime().getMJD() > stopTime.getMJD()) {
                            stopTime = nextInputProduct.getEndTime();
                        }
                        // closing input, delete files in working dir
                        LOG.info("closing " + inputData.getName());
                        if (inputProduct != nextInputProduct) {
                            nextInputProduct.dispose();
                        }
                        dispose();
                        if (inputData.isDirectory()) {
                            FileUtils.deleteDirectory(inputData);
                        } else {
                            inputData.delete();
                        }
                    }
                }
            }

            // bookkeeping of bytes read
            if (getMapContext().getInputSplit() instanceof FileSplit) {
                FileSplit fileSplit = (FileSplit) getMapContext().getInputSplit();
                getMapContext().getCounter("Direct File System Counters", "FILE_SPLIT_BYTES_READ").setValue(fileSplit.getLength());
            }

            LOG.info("completing aggregation");
            targetProduct = aggregator.complete();
            if (getConfiguration().getBoolean(JobConfigNames.CALVALUS_OUTPUT_SUBSETTING, false)) {
                targetProduct = createSubsetFromOutput(targetProduct);
            }
            targetProduct.setStartTime(startTime);
            targetProduct.setEndTime(stopTime);

            getLogger().info(String.format("Processed product width = %d height = %d",
                                           targetProduct.getSceneRasterWidth(),
                                           targetProduct.getSceneRasterHeight()));

        } finally {
            pm.done();
        }
        return true;
    }

    @Override
    public Product openProcessedProduct() {
        return targetProduct;
    }

     @Override
    public boolean supportsPullProcessing() {
        return false;
    }
}
