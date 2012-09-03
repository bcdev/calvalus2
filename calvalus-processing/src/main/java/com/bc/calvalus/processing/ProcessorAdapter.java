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
import com.bc.calvalus.processing.beam.StreamingProductReader;
import com.bc.calvalus.processing.hadoop.FSImageInputStream;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.bc.ceres.core.Assert;
import com.bc.ceres.core.ProgressMonitor;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.datamodel.Product;

import java.awt.Rectangle;
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
    private Geometry roiGeometry;

    public ProcessorAdapter(MapContext mapContext) {
        this.mapContext = mapContext;
        this.inputSplit = mapContext.getInputSplit();
        this.conf = mapContext.getConfiguration();
        Assert.argument(inputSplit instanceof ProductSplit || inputSplit instanceof FileSplit,
                        "input split is neither a FileSplit nor a ProductSplit");
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
     * Returns the names of the products that will we produced by this processor.
     * This should enable fast processing of missing products.
     * <p/>
     * <p/>
     * If the names are not predictable {@code null} will be returned and the processing
     * will always happen.
     *
     * @return The name of the resulting product or {@code null}.
     */
    public String[] getPredictedProductPathes() {
        return null;
    }

    /**
     * Reads and processed a product from the given input split:
     * <ul>
     * <li>read the products</li>
     * <li>creates a subset taking the geometries and processing lines into account (optional)</li>
     * <li>performs a "L2" operation (optional)</li>
     * </ul>
     * the resulting products are contained in the adapter and can be opened using {@code #openProcessedProduct}.
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
    public abstract void saveProcessedProducts(ProgressMonitor pm) throws Exception;

    /**
     * Convenient method that returns the processed product and does all the necessary steps.
     *
     * @param pm A progress monitor
     * @return The processed product
     */
    public Product getProcessedProduct(ProgressMonitor pm) throws IOException { // TODO use pm
        Product processedProduct = openProcessedProduct();
        if (processedProduct == null) {
            Rectangle sourceRectangle = getInputRectangle();
            if (sourceRectangle == null || !sourceRectangle.isEmpty()) {
                int numProducts = processSourceProduct(pm);
                if (numProducts > 0) {
                    processedProduct = openProcessedProduct();
                }
            }
        }
        return processedProduct;
    }

    /**
     * Sets an additional geometry that will be intersected with geometry from the configuration (if any)
     * to define the region to be processed.
     *
     * @param roiGeometry the additional ROI geometry
     */
    public void setAdditionalGeometry(Geometry roiGeometry) {
        this.roiGeometry = roiGeometry;
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
            Geometry regionGeometry = JobUtils.createGeometry(getConfiguration().get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
            ProcessingRectangleCalculator calculator = new ProcessingRectangleCalculator(regionGeometry, roiGeometry, inputSplit) {
                @Override
                Product getProduct() throws IOException {
                    return getInputProduct();
                }
            };
            inputRectangle = calculator.computeRect();
        }
        return inputRectangle;
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

        return readProduct(getInputPath(), inputFormat);
    }

    /**
     * Reads a product from the distributed file system.
     *
     * @param inputPath   The input path
     * @param inputFormat The input format, may be {@code null}. If {@code null}, the file format will be detected.
     * @return The product The product read.
     * @throws java.io.IOException If an I/O error occurs
     */
    private Product readProduct(Path inputPath, String inputFormat) throws IOException {
        Configuration configuration = getConfiguration();
        final Product product;
        if ("HADOOP-STREAMING".equals(inputFormat) || inputPath.getName().toLowerCase().endsWith(".seq")) {
            StreamingProductReader reader = new StreamingProductReader(inputPath, configuration);
            product = reader.readProductNodes(null, null);
        } else {
            // first try a fast,direct ImageInputStream
            Object input = openImageInputStream(inputPath);
            ProductReader productReader = ProductIO.getProductReaderForInput(input);
            if (productReader == null) {
                // try a local file copy
                input = copyProductToLocal(inputPath);
                productReader = ProductIO.getProductReaderForInput(input);
                if (productReader == null) {
                    throw new IOException(String.format("No reader found for product: '%s'", inputPath.toString()));
                }
            }
            product = productReader.readProductNodes(input, null);
        }
        if (product == null) {
            throw new IOException(String.format("No reader found for product '%s' using input format '%s'", inputPath.toString(), inputFormat));
        }
        getLogger().info(String.format("Opened product width = %d height = %d",
                                       product.getSceneRasterWidth(),
                                       product.getSceneRasterHeight()));
        return product;
    }

    /**
     * Copies the product given to the local input directory for access as a ordinary {@code eFile}.
     *
     * @param inputPath The path to the product in the HDFS.
     * @return the local file that contains the copy.
     * @throws IOException
     */
    protected File copyProductToLocal(Path inputPath) throws IOException {
        File inputDir = new File("input");
        inputDir.mkdirs();
        File localFile = new File(inputDir, inputPath.getName());
        if (!localFile.exists()) {
            FileSystem fs = inputPath.getFileSystem(conf);
            FileUtil.copy(fs, inputPath, localFile, false, conf);
        }
        return localFile;
    }

    protected Object openImageInputStream(Path inputPath) throws IOException {
        FileSystem fs = inputPath.getFileSystem(conf);
        final FileStatus status = fs.getFileStatus(inputPath);
        final FSDataInputStream in = fs.open(inputPath);
        return new FSImageInputStream(in, status.getLen());
    }

    /**
     * Disposes the resources allocated by this processor adapter.
     * All products opened or processed by this adapter are disposed as well.
     */
    public void dispose() {
        if (inputProduct != null) {
            inputProduct.dispose();
            inputProduct = null;
        }
    }
}
