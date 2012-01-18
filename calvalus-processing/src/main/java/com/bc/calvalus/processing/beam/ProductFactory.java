/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
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


import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.hadoop.FSImageInputStream;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.bc.calvalus.processing.xml.XmlBinding;
import com.bc.ceres.binding.BindingException;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.dataio.ProductReaderPlugIn;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.framework.gpf.Operator;
import org.esa.beam.framework.gpf.OperatorSpi;
import org.esa.beam.gpf.operators.standard.SubsetOp;
import org.esa.beam.util.SystemUtils;

import javax.imageio.stream.ImageInputStream;
import javax.media.jai.JAI;

import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A factory for products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class ProductFactory {
    private static final int M = 1024 * 1024;
    public static final int DEFAULT_TILE_CACHE_SIZE = 512 * M; // 512 M

    private static final Logger LOG = CalvalusLogger.getLogger();


    private final Configuration configuration;
    private List<File> productCaches;
    private boolean allowSpatialSubset = true;

    /**
     * Constructor.
     *
     * @param configuration The Hadoop job configuration
     */
    public ProductFactory(Configuration configuration) {
        this.configuration = configuration;
        this.productCaches = new ArrayList<File>();
        initGpf(configuration, this.getClass().getClassLoader());
    }

    public static void initGpf(Configuration configuration, ClassLoader classLoader) {
        initSystemProperties(configuration);
        SystemUtils.init3rdPartyLibs(classLoader);
        JAI.enableDefaultTileCache();
        JAI.getDefaultInstance().getTileCache().setMemoryCapacity(configuration.getLong(JobConfigNames.CALVALUS_BEAM_TILE_CACHE_SIZE, DEFAULT_TILE_CACHE_SIZE));
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
    }

    private static void initSystemProperties(Configuration configuration) {
        for (Map.Entry<String, String> entry : configuration) {
            String key = entry.getKey();
            if (key.startsWith("calvalus.system.")) {
                String propertyName = key.substring("calvalus.system.".length());
                String propertyValue = entry.getValue();
                LOG.info(String.format("Setting system property: %s=%s", propertyName, propertyValue));
                System.setProperty(propertyName, propertyValue);
            }
        }
    }

    public void setAllowSpatialSubset(boolean allowSpatialSubset) {
        this.allowSpatialSubset = allowSpatialSubset;
    }

    // todo - nf/nf 19.04.2011: generalise following L2 processor call, so that we can also call 'l2gen'

    /**
     * Reads and processed a product from the given input split:
     * <ul>
     * <li>read the products</li>
     * <li>creates a subset taking the geometry and processing lines into account (optional)</li>
     * <li>performs a "L2" operation (optional)</li>
     * </ul>
     * finally the resulting product is returned.
     * <p/>
     * Currently only GPF operator names are supported.
     *
     * @param inputSplit The input split, must be either a FileSplit or a ProductSplit.
     * @return The product corresponding to the given input path, region geometry and processor.
     * @throws java.io.IOException If an I/O error occurs
     */
    public Product getProcessedProduct(InputSplit inputSplit) throws IOException {
        final Path inputPath;
        int startLine = 0;
        int stopLine = 0;
        if (inputSplit instanceof ProductSplit) {
            ProductSplit productSplit = (ProductSplit) inputSplit;
            inputPath = productSplit.getPath();
            startLine = productSplit.getStartLine();
            stopLine = productSplit.getStopLine();
        } else if (inputSplit instanceof FileSplit) {
            FileSplit fileSplit = (FileSplit) inputSplit;
            inputPath = fileSplit.getPath();
        } else {
            throw new IllegalArgumentException("input split is neither a FileSplit nor a ProductSplit");
        }

        String inputFormat = configuration.get(JobConfigNames.CALVALUS_INPUT_FORMAT, null);
        Geometry regionGeometry = JobUtils.createGeometry(configuration.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        String processorName = configuration.get(JobConfigNames.CALVALUS_L2_OPERATOR);
        String processorParameters = configuration.get(JobConfigNames.CALVALUS_L2_PARAMETERS);

        Product sourceProduct = readProduct(inputPath, inputFormat);
        Product targetProduct;
        try {
            targetProduct = getProcessedProduct(sourceProduct, regionGeometry, allowSpatialSubset, startLine, stopLine, processorName, processorParameters);
            if (targetProduct == null) {
                sourceProduct.dispose();
            }
        } catch (RuntimeException t) {
            sourceProduct.dispose();
            throw t;
        }
        return targetProduct;
    }

    /**
     * Reads a product from the distributed file system.
     *
     * @param inputPath   The input path
     * @param inputFormat The input format, may be {@code null}. If {@code null}, the file format will be detected.
     * @return The product The product read.
     * @throws java.io.IOException If an I/O error occurs
     */
    public Product readProduct(Path inputPath, String inputFormat) throws IOException {
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
                    File productCacheDir = new File(System.getProperty("java.io.tmpdir"), String.format("product-cache-%d", productCaches.size()));
                    productCacheDir.mkdirs();
                    productCaches.add(productCacheDir);
                    File tmpFile = new File(productCacheDir, inputPath.getName());
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
        LOG.info(String.format("Opened product width = %d height = %d",
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


    public void dispose() throws IOException {
        if (productCaches != null) {
            for (File cacheDir : productCaches) {
                FileUtil.fullyDelete(cacheDir);
            }
            productCaches = null;
        }
    }

    public static Map<String, Object> getOperatorParameterMap(String operatorName, String level2Parameters) throws BindingException {
        if (level2Parameters == null) {
            return Collections.emptyMap();
        }
        Class<? extends Operator> operatorClass = getOperatorClass(operatorName);
        return new XmlBinding().convertXmlToMap(level2Parameters, operatorClass);
    }

    private static Class<? extends Operator> getOperatorClass(String operatorName) {
        OperatorSpi operatorSpi = GPF.getDefaultInstance().getOperatorSpiRegistry().getOperatorSpi(operatorName);
        if (operatorSpi == null) {
            throw new IllegalStateException(String.format("Unknown operator '%s'", operatorName));
        }
        return operatorSpi.getOperatorClass();
    }

    static Product getSubsetProduct(Product product, Geometry regionGeometry,
                                    boolean allowSpatialSubset, int startLine, int stopLine) {
        Rectangle pixelRegion = new Rectangle(product.getSceneRasterWidth(), product.getSceneRasterHeight());
        if (!(regionGeometry == null || regionGeometry.isEmpty() || isGlobalCoverageGeometry(regionGeometry))) {
            try {
                pixelRegion = SubsetOp.computePixelRegion(product, regionGeometry, 1);
            } catch (Exception e) {
                // Computation of pixel region could fail (JTS Exception), if the geo-coding of the product is messed up
                // in this case ignore this product
                return null;
            }
        }
        // adjust region to start/stop line
        if (startLine != -1 && stopLine != -1) {
            final int width = product.getSceneRasterWidth();
            final int height = stopLine - startLine + 1;
            pixelRegion = pixelRegion.intersection(new Rectangle(0, startLine, width, height));
        }
        //  SubsetOp throws an OperatorException if pixelRegion.isEmpty(), we don't want this
        if (pixelRegion.isEmpty()) {
            return null;
        }
        // full region
        if (!allowSpatialSubset || (pixelRegion.width == product.getSceneRasterWidth() && pixelRegion.height == product.getSceneRasterHeight())) {
            return product;
        }

        final SubsetOp op = new SubsetOp();
        op.setSourceProduct(product);
        op.setRegion(pixelRegion);
        op.setCopyMetadata(false);
        Product subsetProduct = op.getTargetProduct();
        LOG.info(String.format("Created Subset product width = %d height = %d",
                               subsetProduct.getSceneRasterWidth(),
                               subsetProduct.getSceneRasterHeight()));
        return subsetProduct;
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


    private static Product getProcessedProduct(Product source, String operatorName, String operatorParameters) {
        Product product = source;
        if (operatorName != null && !operatorName.isEmpty()) {
            // transform request into parameter objects
            Map<String, Object> parameterMap;
            try {
                parameterMap = getOperatorParameterMap(operatorName, operatorParameters);
            } catch (BindingException e) {
                throw new IllegalArgumentException("Invalid operator parameters: " + e.getMessage(), e);
            }
            product = GPF.createProduct(operatorName, parameterMap, product);
        }
        return product;
    }

    /**
     * Generates a target product using the given parameters.
     * {@code processorName} may be the name of a Unix executable, a BEAM GPF operator or GPF XML processing graph.
     * Currently only GPF operator names are supported.
     */
    static Product getProcessedProduct(Product sourceProduct,
                                       Geometry regionGeometry,
                                       boolean allowSpatialSubset,
                                       int startLine,
                                       int stopLine,
                                       String processorName,
                                       String processorParameters) {
        Product subsetProduct = getSubsetProduct(sourceProduct, regionGeometry, allowSpatialSubset, startLine, stopLine);
        if (subsetProduct == null) {
            return null;
        }
        Product processedProduct = getProcessedProduct(subsetProduct, processorName, processorParameters);
        if (processedProduct != null && processedProduct != subsetProduct) {
            if (processedProduct.getStartTime() == null) {
                processedProduct.setStartTime(subsetProduct.getStartTime());
            }
            if (processedProduct.getEndTime() == null) {
                processedProduct.setEndTime(subsetProduct.getEndTime());
            }
            LOG.info(String.format("Processed product width = %d height = %d",
                                   processedProduct.getSceneRasterWidth(),
                                   processedProduct.getSceneRasterHeight()));
        }
        return processedProduct;
    }


}
