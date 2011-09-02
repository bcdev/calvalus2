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
import com.bc.calvalus.processing.xml.XmlBinding;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.framework.gpf.Operator;
import org.esa.beam.framework.gpf.OperatorSpi;
import org.esa.beam.gpf.operators.standard.SubsetOp;
import org.esa.beam.util.SystemUtils;

import javax.imageio.stream.ImageInputStream;
import javax.media.jai.JAI;
import java.awt.*;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collections;
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

    /**
     * Constructor.
     *
     * @param configuration The Hadoop job configuration
     */
    public ProductFactory(Configuration configuration) {
        this.configuration = configuration;
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

    // todo - nf/nf 19.04.2011: generalise following L2 processor call, so that we can also call 'l2gen'

    /**
     * Reads a source product and generates a target product using the given parameters.
     * {@code processorName} may be the name of a Unix executable, a BEAM GPF operator or GPF XML processing graph.
     * Currently only GPG operator names are supported.
     *
     * @param regionGeometryWkt   The geometry of the region of interest given as WKT. May be {@code null} or empty.
     * @param processorName       The name of a processor. May be {@code null} or empty.
     * @param processorParameters The text-encoded parameters for the processor.
     * @param inputPath           The input path
     * @param inputFormat         The input format
     * @return The target product.
     * @throws java.io.IOException If an I/O error occurs
     */
    public Product getProduct(Path inputPath,
                              String inputFormat,
                              String regionGeometryWkt,
                              String processorName,
                              String processorParameters) throws IOException {

        Product sourceProduct = readProduct(inputPath, inputFormat, configuration);
        Product targetProduct;
        try {
            targetProduct = getProcessedProduct(sourceProduct, regionGeometryWkt, processorName, processorParameters);
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
     * @param inputPath     The input path
     * @param inputFormat   The input format
     * @param configuration the configuration
     * @return The product
     * @throws java.io.IOException If an I/O error occurs
     */
    private static Product readProduct(Path inputPath, String inputFormat, Configuration configuration) throws IOException {
        final FileSystem fs = inputPath.getFileSystem(configuration);
        final Product product;
        if (inputFormat.equals("HADOOP-STREAMING")) {
            StreamingProductReader reader = new StreamingProductReader(inputPath, configuration);
            product = reader.readProductNodes(null, null);
        } else {
            final FileStatus status = fs.getFileStatus(inputPath);
            final FSDataInputStream in = fs.open(inputPath);
            final ImageInputStream imageInputStream = new FSImageInputStream(in, status.getLen());
            ProductReader productReader = ProductIO.getProductReader(inputFormat);
            if (productReader != null) {
                product = productReader.readProductNodes(imageInputStream, null);
            } else {
                product = null;
            }
        }
        if (product == null) {
            throw new IOException(MessageFormat.format("No reader found for product '{0}' using input format '{1}'", inputPath, inputFormat));
        }
        return product;
    }

    public static Map<String, Object> getOperatorParameterMap(String operatorName, String level2Parameters) {
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

    private static Product getSubsetProduct(Product product, String regionGeometryWkt) {
        final Geometry regionGeometry = JobUtils.createGeometry(regionGeometryWkt);
        if (regionGeometry == null || regionGeometry.isEmpty()) {
            return product;
        }
        final Rectangle pixelRegion;
        try {
            pixelRegion = SubsetOp.computePixelRegion(product, regionGeometry, 1);
        } catch (Exception e) {
            // Computation of pixel region could fail (JTS Exception), if the geo-coding of the product is messed up
            // in this case ignore this product
            return null;
        }
        //  SubsetOp throws an OperatorException if pixelRegion.isEmpty(), we don't want this
        if (pixelRegion.isEmpty()) {
            return null;
        }

        final SubsetOp op = new SubsetOp();
        op.setSourceProduct(product);
        op.setRegion(pixelRegion);
        op.setCopyMetadata(false);
        return op.getTargetProduct();
    }

    private static Product getProcessedProduct(Product source, String operatorName, String operatorParameters) {
        Product product = source;
        if (operatorName != null && !operatorName.isEmpty()) {
            // transform request into parameter objects
            Map<String, Object> parameterMap = getOperatorParameterMap(operatorName, operatorParameters);
            product = GPF.createProduct(operatorName, parameterMap, product);
        }
        return product;
    }

    static Product getProcessedProduct(Product sourceProduct,
                                       String regionGeometryWkt,
                                       String processorName,
                                       String processorParameters) {
        Product subsetProduct = getSubsetProduct(sourceProduct, regionGeometryWkt);
        if (subsetProduct == null) {
            return null;
        }
        Product targetProduct = getProcessedProduct(subsetProduct, processorName, processorParameters);
        if (targetProduct != null && targetProduct != subsetProduct) {
            if (targetProduct.getStartTime() == null) {
                targetProduct.setStartTime(subsetProduct.getStartTime());
            }
            if (targetProduct.getEndTime() == null) {
                targetProduct.setEndTime(subsetProduct.getEndTime());
            }
        }
        return targetProduct;
    }


}
