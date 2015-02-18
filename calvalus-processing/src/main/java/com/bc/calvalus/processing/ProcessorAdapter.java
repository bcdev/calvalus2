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

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.beam.GpfUtils;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.bc.ceres.core.ProgressMonitor;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Adapts different processors ( BEAM GPF, Shell executable, ...) to Calvalus Map-Reduce processing.
 * Usage, simple version:
 * <pre>
 * ProcessorAdapter processorAdapter = ProcessorFactory.create(context);
 * try {
 *     Product target = processorAdapter.getProcessedProduct();
 *     ....
 * } finally {
 *     processorAdapter.dispose();
 * }
 * </pre>
 * <p/>
 * If more control is required, further adjust the processed region, ...):
 * <pre>
 * ProcessorAdapter processorAdapter = ProcessorFactory.create(context);
 * try {
 *     // use points from reference data set to restrict roi even further
 *     Product inputProduct = processorAdapter.getInputProduct();
 *     Geometry roi; // from config
 *     Geometry referenceDataRoi; // from reference data
 *     roi = roi.intersection(referenceDataRoi);
 *
 *     Rectangle srcProductRect = processorAdapter.computeIntersection(roi);
 *     if (!srcProductRect.isEmpty()) {
 *         processorAdapter.processSourceProduct(srcProductRect);
 *
 *         // depending on the requirements:
 *         // save the result to HDFS
 *         processorAdapter.saveProcessedProducts();
 *
 *         // or work with the resulting product
 *         Product processedProduct = processorAdapter.openProcessedProduct();
 *     }
 * } finally {
 *     processorAdapter.dispose();
 * }
 * </pre>
 *
 * @author MarcoZ
 */
public abstract class ProcessorAdapter {

    private static final Logger LOG = CalvalusLogger.getLogger();

    private final MapContext mapContext;
    private final Configuration conf;
    private final InputSplit inputSplit;

    private Product inputProduct;
    private Rectangle inputRectangle;
    private Rectangle roiRectangle;
    private File inputFile;
    private AffineTransform input2OutputTransform;

    public ProcessorAdapter(MapContext mapContext) {
        this.mapContext = mapContext;
        this.inputSplit = mapContext.getInputSplit();
        this.conf = mapContext.getConfiguration();
        GpfUtils.init(mapContext.getConfiguration());
    }

    protected MapContext getMapContext() {
        return mapContext;
    }

    protected Configuration getConfiguration() {
        return conf;
    }

    protected Logger getLogger() {
        return LOG;
    }

    /**
     * Prepares the processing.
     * The default implementation does nothing.
     */
    public void prepareProcessing() throws IOException {

    }

    /**
     * Returns whether the adapter can skip processing the current input product.
     * The adapters answer should be based on information about the input
     * product and whether the corresponding output product already exists.
     * <p/>
     * Before this method is called prepare processing has to be invoked!
     * <p/>
     * This should enable fast (re-)processing of missing products.
     *
     * @return {@code true}, if the input product should not be processed.
     */
    public boolean canSkipInputProduct() throws IOException {
        return false;
    }

    /**
     * Reads and processed a product from the given input split:
     * <ul>
     * <li>read the products</li>
     * <li>creates a subset taking the geometries and processing lines into account (optional)</li>
     * <li>performs a "L2" operation (optional)</li>
     * </ul>
     * the resulting products are contained in the adapter and can be opened using {@code #openProcessedProduct}.
     * <p/>
     * Before this method is called prepare processing has to be invoked!
     * <p/>
     *
     * @param pm A progress monitor
     * @return The number of processed products.
     * @throws java.io.IOException If an I/O error occurs
     */
    public abstract int processSourceProduct(ProgressMonitor pm) throws IOException;

    /**
     * Returns the product resulting from the processing.
     * Before this method can be called the {@code #processSourceProduct} method must be called.
     * <p/>
     * TODO use index to get all processed products
     *
     * @return The processed product
     */
    public abstract Product openProcessedProduct() throws IOException;

    /**
     * Saves the processed products onto HDFS.
     *
     * @param pm A progress monitor
     * @throws java.io.IOException If an I/O error occurs
     */
    public abstract void saveProcessedProducts(ProgressMonitor pm) throws IOException;

    /**
     * Return the output path to the processed product.
     * Can return {@code null} if no output product exist (yet).
     *
     * @return The output path of the output product.
     */
    public abstract Path getOutputProductPath() throws IOException;

    protected Path getOutputDirectoryPath() throws IOException {
        Path outputPath = FileOutputFormat.getOutputPath(getMapContext());
        return appendDatePart(outputPath);
    }

    protected Path getWorkOutputDirectoryPath() throws IOException {
        try {
            Path workOutputPath = FileOutputFormat.getWorkOutputPath(getMapContext());
            return appendDatePart(workOutputPath);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    Path appendDatePart(Path path) {
        if (getConfiguration().getBoolean(JobConfigNames.CALVALUS_OUTPUT_PRESERVE_DATE_TREE, false)) {
            String datePart = getDatePart(getInputPath());
            if (datePart != null) {
                path = new Path(path, datePart);
            }
        }
        return path;
    }

    /**
     * @param inputProductPath the path to the input product
     * @return the "year/month/day" part of the inputProductPath,
     * returns {@null}, if the input product path contains no date part
     */
    static String getDatePart(Path inputProductPath) {
        Path day = inputProductPath.getParent();
        if (day != null && !day.getName().isEmpty()) {
            Path month = day.getParent();
            if (month != null && !month.getName().isEmpty()) {
                Path year = month.getParent();
                if (year != null && !year.getName().isEmpty()) {
                    return year.getName() + "/" + month.getName() + "/" + day.getName();
                }
            }
        }
        return null;
    }


    /**
     * Return {code true}, if the processor adapter supports on-demand processing of distinct regions.
     *
     * @return {code true}, if pull processing is supported.
     */
    public abstract boolean supportsPullProcessing();

    /**
     * Convenient method that returns the processed product and does all the necessary steps.
     *
     * @param pm A progress monitor
     * @return The processed product
     */
    public Product getProcessedProduct(ProgressMonitor pm) throws IOException {
        Product processedProduct = openProcessedProduct();
        if (processedProduct == null) {
            Rectangle sourceRectangle = getInputRectangle();
            if (sourceRectangle == null || !sourceRectangle.isEmpty()) {
                prepareProcessing();
                int numProducts = processSourceProduct(pm);
                if (numProducts > 0) {
                    processedProduct = openProcessedProduct();
                }
            }
        }
        return processedProduct;
    }

    /**
     * Sets an additional rectangle in pixel coordinates that will be intersected with
     * the geometry from the configuration (if any)
     * to define the region to be processed.
     *
     * @param roiRectangle the additional ROI rectangle
     */
    public void setProcessingRectangle(Rectangle roiRectangle) {
        this.roiRectangle = roiRectangle;
        inputRectangle = null;
    }

    /**
     * Return the path to the input product.
     *
     * @return The path of the input product.
     */
    public Path getInputPath() {
        if (inputSplit instanceof ProductSplit) {
            ProductSplit productSplit = (ProductSplit) inputSplit;
            return productSplit.getPath();
        } else if (inputSplit instanceof FileSplit) {
            FileSplit fileSplit = (FileSplit) inputSplit;
            return fileSplit.getPath();
        } else {
            throw new IllegalArgumentException("input split is neither a FileSplit nor a ProductSplit");
        }
    }

    /**
     * Return the region of the input product that is processed (in pixel coordinates).
     * This takes the geometries given in the configuration the additionalGeometry and processing lines into account.
     *
     * @return The input region to process, or {@code null} if no restriction is given.
     */
    public Rectangle getInputRectangle() throws IOException {
        if (inputRectangle == null) {
            String geometryWkt = getConfiguration().get(JobConfigNames.CALVALUS_REGION_GEOMETRY);
            boolean fullSwath = getConfiguration().getBoolean(JobConfigNames.CALVALUS_INPUT_FULL_SWATH, false);
            Geometry regionGeometry = JobUtils.createGeometry(geometryWkt);
            ProcessingRectangleCalculator calculator = new ProcessingRectangleCalculator(regionGeometry,
                                                                                         roiRectangle,
                                                                                         inputSplit,
                                                                                         fullSwath) {
                @Override
                Product getProduct() throws IOException {
                    return getInputProduct();
                }
            };
            inputRectangle = calculator.computeRect();
            LOG.info("computed inputRectangle = " + inputRectangle);
        }
        return inputRectangle;
    }

    public AffineTransform getInput2OutputTransform() {
        return input2OutputTransform;
    }

    public void setInput2OutputTransform(AffineTransform input2OutputTransform) {
        this.input2OutputTransform = input2OutputTransform;
    }

    /**
     * Return the input product.
     *
     * @return The input product
     * @throws java.io.IOException If an I/O error occurs
     */
    public Product getInputProduct() throws IOException {
        if (inputProduct == null) {
            inputProduct = openInputProduct();
        }
        return inputProduct;
    }

    private Product openInputProduct() throws IOException {
        Configuration conf = getConfiguration();
        String inputFormat = conf.get(JobConfigNames.CALVALUS_INPUT_FORMAT, null);
        if (inputFile != null) {
            if (inputFormat != null) {
                return ProductIO.readProduct(inputFile, inputFormat);
            } else {
                return ProductIO.readProduct(inputFile);
            }
        } else {
            return CalvalusProductIO.readProduct(getInputPath(), getConfiguration(), inputFormat);
        }
    }

    public void setInputFile(File inputFile) {
        this.inputFile = inputFile;
    }

    public File getInputFile() {
        return inputFile;
    }

    /**
     * Disposes the resources allocated by this processor adapter.
     * All products opened or processed by this adapter are disposed as well.
     */
    public void dispose() {
        closeInputProduct();
    }

    public void closeInputProduct() {
        if (inputProduct != null) {
            inputProduct.dispose();
            inputProduct = null;
        }
    }

    public static boolean hasInvalidStartAndStopTime(Product product) {
        ProductData.UTC startTime = product.getStartTime();
        ProductData.UTC endTime = product.getEndTime();
        if (startTime == null || endTime == null) {
            return true;
        }
        if (endTime.getMJD() == 0.0 || startTime.getMJD() == 0.0) {
            return true;
        }
        return false;
    }

    public static void copySceneRasterStartAndStopTime(Product sourceProduct, Product targetProduct,
                                                       Rectangle inputRectangle) {
        final ProductData.UTC startTime = sourceProduct.getStartTime();
        final ProductData.UTC stopTime = sourceProduct.getEndTime();
        boolean fullHeight = sourceProduct.getSceneRasterHeight() == targetProduct.getSceneRasterHeight();

        if (startTime != null && stopTime != null && !fullHeight && inputRectangle != null) {
            final double height = sourceProduct.getSceneRasterHeight();
            final double regionY = inputRectangle.getY();
            final double regionHeight = inputRectangle.getHeight();
            final double dStart = startTime.getMJD();
            final double dStop = stopTime.getMJD();
            final double vPerLine = (dStop - dStart) / (height - 1);
            final double newStart = vPerLine * regionY + dStart;
            final double newStop = vPerLine * (regionHeight - 1) + newStart;
            targetProduct.setStartTime(new ProductData.UTC(newStart));
            targetProduct.setEndTime(new ProductData.UTC(newStop));
        } else {
            targetProduct.setStartTime(startTime);
            targetProduct.setEndTime(stopTime);
        }
    }
}
