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
import java.awt.Rectangle;
import java.io.IOException;
import java.io.StringReader;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Diverse utilities.
 *
 * @author MarcoZ
 */
public class BeamUtils {
    //TODO make this a configurable option
    private static final int TILE_CACHE_SIZE_M = 800;  // 512 MB

    public static void initGpf(Configuration configuration) {
        SystemUtils.init3rdPartyLibs(JobUtils.class.getClassLoader());
        JAI.enableDefaultTileCache();
        JAI.getDefaultInstance().getTileCache().setMemoryCapacity(TILE_CACHE_SIZE_M * 1024 * 1024);
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
        JobUtils.initSystemProperties(configuration);
    }

    /**
     * Reads a product from the distributed file system.
     *
     * @param inputPath         The input path
     * @param configuration the configuration
     * @return The product
     * @throws java.io.IOException If an I/O error occurs
     */
    public static Product readProduct(Path inputPath, Configuration configuration) throws IOException {
        final FileSystem fs = inputPath.getFileSystem(configuration);
        String inputFormat = configuration.get(JobConfNames.CALVALUS_INPUT_FORMAT, "ENVISAT");
        Product product = null;
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
            }
        }
        if (product == null) {
            throw new IllegalStateException(MessageFormat.format("No reader found for product {0} (inputFormat={1}", inputPath, inputFormat));
        }
        return product;
    }

    public static Map<String, Object> getLevel2ParameterMap(String operatorName, String level2Parameters) {
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

    public static Class<? extends Operator> getOperatorClass(String operatorName) throws ConversionException {
        OperatorSpi operatorSpi = GPF.getDefaultInstance().getOperatorSpiRegistry().getOperatorSpi(operatorName);
        if (operatorSpi == null) {
            throw new ConversionException(MessageFormat.format("Unknown operator ''{0}''", operatorName));
        }
        return operatorSpi.getOperatorClass();
    }

    public static Product createSubsetProduct(Product product, String roiWkt) {
        final Geometry roiGeometry = JobUtils.createGeometry(roiWkt);
        if (roiGeometry == null || roiGeometry.isEmpty()) {
            return product;
        }
        final Rectangle pixelRegion;
        try {
            pixelRegion = SubsetOp.computePixelRegion(product, roiGeometry, 1);
        } catch (Exception e) {
            // computation of pixel region could fail, if the geocoding of the product is messed up
            // in this case ignore this product
            return null;
        }
        if (pixelRegion.isEmpty()) {
            return null;
        }

        final SubsetOp op = new SubsetOp();
        op.setSourceProduct(product);
        op.setRegion(pixelRegion);
        op.setCopyMetadata(false);
        return op.getTargetProduct();
    }

    public static Product getProcessedProduct(Product source, String level2OperatorName, String level2Parameters) {
        Product product = source;
        if (level2OperatorName != null && !level2OperatorName.isEmpty()) {
            // transform request into parameter objects
            Map<String, Object> level2ParameterMap = getLevel2ParameterMap(level2OperatorName, level2Parameters);
            product = GPF.createProduct(level2OperatorName, level2ParameterMap, product);
        }
        return product;
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
