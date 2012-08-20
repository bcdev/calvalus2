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
import com.vividsolutions.jts.geom.Envelope;
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
import org.esa.beam.framework.dataio.ProductReaderPlugIn;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.gpf.operators.standard.SubsetOp;
import org.esa.beam.util.Debug;

import javax.imageio.stream.ImageInputStream;
import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Adapts different processors ( BEAM GPF, Shell executable, ...) to Calvalus Map-Reduce processing.
 * Usage, simple version:
 * <code>
 *   ProcessorAdapter processorAdapter = ProcessorFactory.create(context);
 *   try {
 *       Product target = processorAdapter.getProcessedProduct();
 *       ....
 *   } finally {
 *       processorAdapter.dispose();
 *   }
 * </code>
 *
 * If more control is required, further adjust the processed region, ...):
 * <code>
 *   ProcessorAdapter processorAdapter = ProcessorFactory.create(context);
 *   try {
 *       // use points from reference data set to restrict roi even further
 *       Product inputProduct = processorAdapter.getInputProduct();
 *       Geometry roi; // from config
 *       Geometry referenceDataRoi; // from reference data
 *       roi = roi.intersection(referenceDataRoi);
 *
 *       Rectangle srcProductRect = processorAdapter.computeIntersection(roi);
 *       if (!srcProductRect.isEmpty()) {
 *           processorAdapter.processSourceProduct(srcProductRect);
 *
 *           // depending on the requirements:
 *           // save the result to HDFS
 *           processorAdapter.saveProcessedProduct(mapcontext, outputFilename);
 *
 *           // or work with the resulting product
 *           Product processedProduct = processorAdapter.openProcessedProduct();
 *       }
 *   } finally {
 *       processorAdapter.dispose();
 *   }
 * </code>
 *
 * @author MarcoZ
 */
public abstract class ProcessorAdapter {

    private static final Logger LOG = CalvalusLogger.getLogger();

    private final MapContext mapContext;
    private final Configuration conf;
    private final InputSplit inputSplit;

    private Product inputProduct;

    public ProcessorAdapter(MapContext mapContext) {
        this.mapContext = mapContext;
        this.inputSplit = mapContext.getInputSplit();
        this.conf = mapContext.getConfiguration();
        Assert.argument(inputSplit instanceof ProductSplit || inputSplit instanceof FileSplit,
                        "input split is neither a FileSplit nor a ProductSplit");
    }

    public MapContext getMapContext() {
        return mapContext;
    }

    public Configuration getConfiguration() {
        return conf;
    }

    public Logger getLogger() {
        return LOG;
    }

    /**
     * Returns the names of the products that will we produced by this processor.
     * This should enable fast processing of missing products.
     *
     *
     * If the names are not predictable {@code null} will be returned and the processing
     * will always happen.
     *
     * @return The name of the resulting product or {@code null}.
     */
    public String[] getProcessedProductPathes() {
        return null;
    }

    /**
     * Reads and processed a product from the given input split:
     * <ul>
     * <li>read the products</li>
     * <li>creates a subset taking the rectangle into account (optional)</li>
     * <li>performs a "L2" operation (optional)</li>
     * </ul>
     * the resulting product is contained in the adapter and can be opened using {@code #openProcessedProduct}.
     *
     * @param srcProductRect The region of the source product to be processed, if {@code null} the product will be processed.
     *                       The rectangle can not be empty.
     * @return true, if the processing succeeded.
     * @throws java.io.IOException If an I/O error occurs
     */
    public abstract boolean processSourceProduct(Rectangle srcProductRect) throws IOException;

    /**
     * Returns the product resulting from the processing.
     * Before this method can be called the {@code #processSourceProduct} method must be called.
     *
     * @return The processed product
     */
    public abstract Product openProcessedProduct() throws IOException;

    /**
     * Saves the processed products onto HDFS.
     *
     * @throws java.io.IOException If an I/O error occurs
     */
    public abstract void saveProcessedProducts() throws Exception;

    /**
     * Convenient method that returns the processed product and does all the necessary steps.
     *
     * @param pm A progress monitor
     * @return The processed product
     */
    public Product getProcessedProduct(ProgressMonitor pm) throws IOException { // TODO use pm
        System.out.println("ProcessorAdapter.getProcessedProduct");
        Product processedProduct = openProcessedProduct();
        if (processedProduct == null) {
            Geometry regionGeometry = JobUtils.createGeometry(getConfiguration().get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
            Rectangle sourceRectangle = computeIntersection(regionGeometry);
            if (!sourceRectangle.isEmpty()) {
                boolean sucess = processSourceProduct(sourceRectangle);
                if (sucess) {
                    processedProduct = openProcessedProduct();
                }
            }
        }
        return processedProduct;
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
     * Computes the intersection between the input product and the given geometry. If there is no intersection an empty
     * rectangle will be returned. The pixel region will also take information
     * from the {@code ProductSplit} based on an inventory into account.
     *
     * @param regionGeometry The region, can be {@code null}
     * @return The intersection, never {@code null}
     * @throws IOException
     */
    public Rectangle computeIntersection(Geometry regionGeometry) throws IOException {
        System.out.println("ProcessorAdapter.computeIntersection");
        Product product = getInputProduct();
        Rectangle pixelRegion = new Rectangle(product.getSceneRasterWidth(), product.getSceneRasterHeight());
        if (!(regionGeometry == null || regionGeometry.isEmpty() || isGlobalCoverageGeometry(regionGeometry))) {
            try {
                pixelRegion = SubsetOp.computePixelRegion(product, regionGeometry, 1);
            } catch (Exception e) {
                // Computation of pixel region could fail (JTS Exception), if the geo-coding of the product is messed up
                // in this case ignore this product
                return new Rectangle();
            }
        }
        // adjust region to start/stop line
        if (inputSplit instanceof ProductSplit) {
            ProductSplit productSplit = (ProductSplit) inputSplit;
            final int processStart = productSplit.getProcessStartLine();
            final int processLength = productSplit.getProcessLength();
            if (processLength > 0) {
                final int width = product.getSceneRasterWidth();
                pixelRegion = pixelRegion.intersection(new Rectangle(0, processStart, width, processLength));
            }
        }
        return pixelRegion;
    }

    static boolean isGlobalCoverageGeometry(Geometry geometry) {
        Envelope envelopeInternal = geometry.getEnvelopeInternal();
        return eq(envelopeInternal.getMinX(), -180.0, 1E-8)
                && eq(envelopeInternal.getMaxX(), 180.0, 1E-8)
                && eq(envelopeInternal.getMinY(), -90.0, 1E-8)
                && eq(envelopeInternal.getMaxY(), 90.0, 1E-8);
    }

    private static boolean eq(double x1, double x2, double eps) {
        double delta = x1 - x2;
        return delta > 0 ? delta < eps : -delta < eps;
    }

    /**
     * Return the input product.
     *
     * @return The input product
     * @throws java.io.IOException If an I/O error occurs
     */
    public Product getInputProduct() throws IOException {
        System.out.println("ProcessorAdapter.getInputProduct");
        if (inputProduct == null) {
            inputProduct = openInputProduct();
        }
        return inputProduct;
    }

    private Product openInputProduct() throws IOException {
        System.out.println("ProcessorAdapter.openInputProduct");
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
        System.out.println("ProcessorAdapter.readProduct");
        Configuration configuration = getConfiguration();
        final FileSystem fs = inputPath.getFileSystem(configuration);
        Product product = null;
        if ("HADOOP-STREAMING".equals(inputFormat) || inputPath.getName().toLowerCase().endsWith(".seq")) {
            StreamingProductReader reader = new StreamingProductReader(inputPath, configuration);
            product = reader.readProductNodes(null, null);
        } else {
            ProductReader productReader = ProductIO.getProductReader(inputFormat != null ? inputFormat : "ENVISAT");
            if (productReader != null) {
                ProductReaderPlugIn readerPlugIn = productReader.getReaderPlugIn();
                Object input = null;
                if (canHandle(readerPlugIn, ImageInputStream.class)) {
                    final FileStatus status = fs.getFileStatus(inputPath);
                    final FSDataInputStream in = fs.open(inputPath);
                    input = new FSImageInputStream(in, status.getLen());
                } else if (canHandle(readerPlugIn, File.class)) {
                    File inputDir = new File("input");
                    inputDir.mkdirs();
                    File tmpFile = new File(inputDir, inputPath.getName());
                    FileUtil.copy(fs, inputPath, tmpFile, false, configuration);
                    input = tmpFile;
                }

                if (input != null) {
                    product = productReader.readProductNodes(input, null);
                }
            }
        }
        if (product == null) {
            throw new IOException(String.format("No reader found for product '%s' using input format '%s'", inputPath.toString(), inputFormat));
        }
        getLogger().info(String.format("Opened product width = %d height = %d",
                                       product.getSceneRasterWidth(),
                                       product.getSceneRasterHeight()));
        return product;
    }

    private static boolean canHandle(ProductReaderPlugIn readerPlugIn, Class<?> inputClass) {
        if (readerPlugIn != null) {
            Class[] inputTypes = readerPlugIn.getInputTypes();
            for (Class inputType : inputTypes) {
                if (inputType.isAssignableFrom(inputClass)) {
                    return true;
                }
            }
        }
        return false;
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
