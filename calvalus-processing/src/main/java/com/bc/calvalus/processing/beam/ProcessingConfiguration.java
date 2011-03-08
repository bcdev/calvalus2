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
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.framework.gpf.Operator;
import org.esa.beam.framework.gpf.OperatorSpi;
import org.esa.beam.framework.gpf.annotations.ParameterDescriptorFactory;
import org.esa.beam.util.StringUtils;

import java.io.StringReader;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration constants.
 */
public class ProcessingConfiguration {
    private static final String CALVALUS_IDENTIFIER = "calvalus.identifier";
    private static final String CALVALUS_BUNDLE = "calvalus.bundle";
    private static final String CALVALUS_INPUT = "calvalus.input";
    private static final String CALVALUS_OUTPUT = "calvalus.output";
    private static final String CALVALUS_L2_OPERATOR = "calvalus.l2.operator";
    private static final String CALVALUS_L2_PARAMETER = "calvalus.l2.parameter";
    private static final String CALVALUS_L3_PARAMETER = "calvalus.l3.parameter";
    private static final String CALVALUS_FORMATTER_PARAMETER = "calvalus.formatter.parameter";

    private final Configuration hadoopConf;

    public ProcessingConfiguration(Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    public String[] getInputPath() {
        return hadoopConf.get(CALVALUS_INPUT).split(",");
    }

    public String getOutputPath() {
        return hadoopConf.get(CALVALUS_OUTPUT);
    }

    public String getLevel2Parameters() {
        return hadoopConf.get(CALVALUS_L2_PARAMETER);
    }

    public String getLevel3Parameters() {
        return hadoopConf.get(CALVALUS_L3_PARAMETER);
    }

    public String getLevel2OperatorName() {
        return hadoopConf.get(CALVALUS_L2_OPERATOR);
    }

    public Map<String, Object> getLevel2ParameterMap() {
        try {
            Class<? extends Operator> operatorClass = getOperatorClass();

            Map<String, Object> parameterMap = new HashMap<String, Object>();
            ParameterDescriptorFactory parameterDescriptorFactory = new ParameterDescriptorFactory();
            PropertySet parameterSet = PropertyContainer.createMapBacked(parameterMap, operatorClass, parameterDescriptorFactory);
            DefaultDomConverter domConverter = new DefaultDomConverter(operatorClass, parameterDescriptorFactory);

            DomElement parametersElement = createDomElement(getLevel2Parameters());
            domConverter.convertDomToValue(parametersElement, parameterSet);
            return parameterMap;
        } catch (Exception e) {
            return Collections.emptyMap();
        }

    }

    Class<? extends Operator> getOperatorClass() throws ConversionException {
        String operatorName = getLevel2OperatorName();
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

    public void addWpsParameters(WpsConfig wpsConfig) {
        addIfNotEmpty(CALVALUS_IDENTIFIER, wpsConfig.getIdentifier());
        addIfNotEmpty(CALVALUS_BUNDLE, wpsConfig.getProcessorPackage());
        String[] requestInputPaths = wpsConfig.getRequestInputPaths();
        String inputs = StringUtils.join(requestInputPaths, ",");
        addIfNotEmpty(CALVALUS_INPUT, inputs);
        addIfNotEmpty(CALVALUS_OUTPUT, wpsConfig.getRequestOutputDir());
        addIfNotEmpty(CALVALUS_L2_OPERATOR, wpsConfig.getOperatorName());
        addIfNotEmpty(CALVALUS_L2_PARAMETER, wpsConfig.getLevel2Paramter());
        addIfNotEmpty(CALVALUS_L3_PARAMETER, wpsConfig.getLevel3Paramter());
        addIfNotEmpty(CALVALUS_FORMATTER_PARAMETER, wpsConfig.getFormatterParameter());
    }

    private void addIfNotEmpty(String key, String value) {
        if (value != null && !value.isEmpty()) {
            hadoopConf.set(key, value);
        }
    }
}
