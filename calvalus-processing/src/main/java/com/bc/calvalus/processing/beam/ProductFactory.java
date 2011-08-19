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


import com.bc.calvalus.processing.JobConfNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.hadoop.FSImageInputStream;
import com.bc.ceres.binding.ConversionException;
import com.bc.ceres.binding.PropertyContainer;
import com.bc.ceres.binding.PropertySet;
import com.bc.ceres.binding.dom.DefaultDomConverter;
import com.bc.ceres.binding.dom.DomElement;
import com.bc.ceres.binding.dom.Xpp3DomElement;
import com.thoughtworks.xstream.io.copy.HierarchicalStreamCopier;
import com.thoughtworks.xstream.io.xml.XppDomWriter;
import com.thoughtworks.xstream.io.xml.XppReader;
import com.thoughtworks.xstream.io.xml.xppdom.Xpp3Dom;
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
import org.esa.beam.framework.gpf.annotations.ParameterDescriptorFactory;
import org.esa.beam.gpf.operators.standard.SubsetOp;
import org.esa.beam.util.SystemUtils;

import javax.imageio.stream.ImageInputStream;
import javax.media.jai.JAI;
import java.awt.*;
import java.io.IOException;
import java.io.StringReader;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A factory for products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class ProductFactory {
    private static final int M = 1024 * 1024;
    public static final int DEFAULT_TILE_CACHE_SIZE = 512 * M; // 512 M

    private  final Configuration configuration;

    /**
     * Constructor.
     * @param configuration       The Hadoop job configuration
     */
    public ProductFactory(Configuration configuration) {
        this.configuration = configuration;
        initGpf(configuration, this.getClass().getClassLoader());
    }

    public static void initGpf(Configuration configuration, ClassLoader classLoader) {
        SystemUtils.init3rdPartyLibs(classLoader);
        JAI.enableDefaultTileCache();
        JAI.getDefaultInstance().getTileCache().setMemoryCapacity(configuration.getLong(JobConfNames.CALVALUS_BEAM_TILE_CACHE_SIZE, DEFAULT_TILE_CACHE_SIZE));
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
        JobUtils.initSystemProperties(configuration);
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
        try {
            Class<? extends Operator> operatorClass = getOperatorClass(operatorName);

            Map<String, Object> parameterMap = new HashMap<String, Object>();
            ParameterDescriptorFactory parameterDescriptorFactory = new ParameterDescriptorFactory();
            PropertySet parameterSet = PropertyContainer.createMapBacked(parameterMap, operatorClass, parameterDescriptorFactory);
            parameterSet.setDefaultValues();
            DefaultDomConverter domConverter = new DefaultDomConverter(operatorClass, parameterDescriptorFactory);
            DomElement parametersElement = createDomElement(level2Parameters);
            domConverter.convertDomToValue(parametersElement, parameterSet);
            return parameterMap;
        } catch (Exception e) {
            return Collections.emptyMap();
        }

    }

    private static Class<? extends Operator> getOperatorClass(String operatorName) throws ConversionException {
        OperatorSpi operatorSpi = GPF.getDefaultInstance().getOperatorSpiRegistry().getOperatorSpi(operatorName);
        if (operatorSpi == null) {
            throw new ConversionException(MessageFormat.format("Unknown operator ''{0}''", operatorName));
        }
        return operatorSpi.getOperatorClass();
    }

    private static Product createSubsetProduct(Product product, String regionGeometryWkt) {
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

    private static Product getProcessedProduct(Product sourceProduct,
                                               String regionGeometryWkt,
                                               String processorName,
                                               String processorParameters) {
        Product subsetProduct = ProductFactory.createSubsetProduct(sourceProduct, regionGeometryWkt);
        if (subsetProduct == null) {
            return null;
        }
        Product targetProduct = ProductFactory.getProcessedProduct(subsetProduct, processorName, processorParameters);
        if (targetProduct != null) {
            if (targetProduct.getStartTime() == null) {
                targetProduct.setStartTime(subsetProduct.getStartTime());
            }
            if (targetProduct.getEndTime() == null) {
                targetProduct.setEndTime(subsetProduct.getEndTime());
            }
        }
        return targetProduct;
    }


    public static DomElement createDomElement(String xml) {
        XppDomWriter domWriter = new XppDomWriter();
        new HierarchicalStreamCopier().copy(new XppReader(new StringReader(xml)), domWriter);
        Xpp3Dom xpp3Dom = domWriter.getConfiguration();
        return new Xpp3DomElement(xpp3Dom);
    }

    public static void convertXmlToObject(String xml, Object object) {
        DomElement domElement = createDomElement(xml);
        ParameterDescriptorFactory parameterDescriptorFactory = new ParameterDescriptorFactory();
        PropertySet parameterSet = PropertyContainer.createObjectBacked(object, parameterDescriptorFactory);
        parameterSet.setDefaultValues();
        DefaultDomConverter domConverter = new DefaultDomConverter(object.getClass(), parameterDescriptorFactory);

        try {
            domConverter.convertDomToValue(domElement, parameterSet);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot convert DOM to Value : " + e.getMessage(), e);
        }
    }

    public static String convertObjectToXml(Object object) {
        ParameterDescriptorFactory parameterDescriptorFactory = new ParameterDescriptorFactory();
        DefaultDomConverter domConverter = new DefaultDomConverter(object.getClass(), parameterDescriptorFactory);

        try {
            DomElement parametersDom = new Xpp3DomElement("parameters");
            domConverter.convertValueToDom(object, parametersDom);
            return parametersDom.toXml();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot convert DOM to Value : " + e.getMessage(), e);
        }
    }

}
