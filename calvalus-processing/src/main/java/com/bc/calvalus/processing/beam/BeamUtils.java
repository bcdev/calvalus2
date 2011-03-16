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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.beam.dataio.envisat.EnvisatProductReaderPlugIn;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.framework.gpf.Operator;
import org.esa.beam.framework.gpf.OperatorSpi;
import org.esa.beam.framework.gpf.annotations.ParameterDescriptorFactory;
import org.esa.beam.util.SystemUtils;

import javax.imageio.stream.ImageInputStream;
import javax.media.jai.JAI;
import java.io.IOException;
import java.io.StringReader;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BeamUtils {
    //TODO make this a configurable option
    private static final int TILE_HEIGHT = 64;
    private static final int TILE_CACHE_SIZE_M = 512;  // 512 MB

    static void initGpf() {
        SystemUtils.init3rdPartyLibs(BeamUtils.class.getClassLoader());
        JAI.enableDefaultTileCache();
        JAI.getDefaultInstance().getTileCache().setMemoryCapacity(TILE_CACHE_SIZE_M * 1024 * 1024);
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
    }

    //TODO use conf fo extract value
    static int getTileHeight(Configuration conf) {
        return TILE_HEIGHT;
    }

    /**
     * Reads a product from the distributed file system.
     *
     * @param inputPath         The input path
     * @param configuration the configuration
     * @return The product
     * @throws java.io.IOException
     */
    static Product readProduct(Path inputPath, Configuration configuration) throws IOException {
        final FileSystem fs = inputPath.getFileSystem(configuration);
        final FileStatus status = fs.getFileStatus(inputPath);
        final FSDataInputStream in = fs.open(inputPath);
        final ImageInputStream imageInputStream = new FSImageInputStream(in, status.getLen());
        System.setProperty("beam.envisat.tileHeight", Integer.toString(getTileHeight(configuration)));
        System.setProperty("beam.envisat.tileWidth", "*");
        final EnvisatProductReaderPlugIn plugIn = new EnvisatProductReaderPlugIn();
        final ProductReader productReader = plugIn.createReaderInstance();
        Product product = productReader.readProductNodes(imageInputStream, null);
        if (product == null) {
            throw new IllegalStateException(MessageFormat.format("No reader found for product {0}", inputPath));
        }
        return product;
    }

    static Map<String, Object> getLevel2ParameterMap(String operatorName, String level2Parameters) {
        try {
            Class<? extends Operator> operatorClass = getOperatorClass(operatorName);

            Map<String, Object> parameterMap = new HashMap<String, Object>();
            ParameterDescriptorFactory parameterDescriptorFactory = new ParameterDescriptorFactory();
            PropertySet parameterSet = PropertyContainer.createMapBacked(parameterMap, operatorClass, parameterDescriptorFactory);
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

    static DomElement createDomElement(String xml) {
        XppDomWriter domWriter = new XppDomWriter();
        new HierarchicalStreamCopier().copy(new XppReader(new StringReader(xml)), domWriter);
        Xpp3Dom xpp3Dom = domWriter.getConfiguration();
        return new Xpp3DomElement(xpp3Dom);
    }

    static void loadFromXml(String xml, Object object) {
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

    public static String saveAsXml(Object object) {
        ParameterDescriptorFactory parameterDescriptorFactory = new ParameterDescriptorFactory();
        PropertySet parameterSet = PropertyContainer.createObjectBacked(object, parameterDescriptorFactory);
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
