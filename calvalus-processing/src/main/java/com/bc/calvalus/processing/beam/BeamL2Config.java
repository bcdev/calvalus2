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
import com.bc.ceres.binding.ValidationException;
import com.bc.ceres.binding.dom.DefaultDomConverter;
import com.bc.ceres.binding.dom.DomElement;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.framework.gpf.Operator;
import org.esa.beam.framework.gpf.OperatorSpi;
import org.esa.beam.framework.gpf.annotations.ParameterDescriptorFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathExpressionException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

public class BeamL2Config {

    private static final String OPERATOR_NAME_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.l2.operator']/Data/LiteralData";
    private static final String OPERATOR_PARAMETERS_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.l2.parameters']/Data/ComplexData/parameters";

    private final WpsConfig wpsConfig;

    public BeamL2Config(WpsConfig wpsConfig) {
        this.wpsConfig = wpsConfig;
    }

    public WpsConfig getWpsConfig() {
        return wpsConfig;
    }

    public String getOperatorName() {
        try {
            return wpsConfig.getRequestXmlDoc().getString(OPERATOR_NAME_XPATH);
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression", e);
        }
    }

    /**
     * Transforms request into parameter objects.
     *
     * @return map of opererator parameters
     */
    public Map<String, Object> getOperatorParameters() throws ValidationException, ConversionException {
        Class<? extends Operator> operatorClass = getOperatorClass();

        Map<String, Object> parameterMap = new HashMap<String, Object>();
        ParameterDescriptorFactory parameterDescriptorFactory = new ParameterDescriptorFactory();
        PropertySet parameterSet = PropertyContainer.createMapBacked(parameterMap, operatorClass, parameterDescriptorFactory);
        DefaultDomConverter domConverter = new DefaultDomConverter(operatorClass, parameterDescriptorFactory);

        DomElement parametersElement = getOperatorParametersDomElement();
        domConverter.convertDomToValue(parametersElement, parameterSet);

        return parameterMap;
    }

    private Class<? extends Operator> getOperatorClass() throws ConversionException {
        String operatorName = getOperatorName();
        OperatorSpi operatorSpi = GPF.getDefaultInstance().getOperatorSpiRegistry().getOperatorSpi(operatorName);
        if (operatorSpi == null) {
            throw new ConversionException(MessageFormat.format("Unknown operator ''{0}''", operatorName));
        }
        return operatorSpi.getOperatorClass();
    }

    DomElement getOperatorParametersDomElement() {
        try {
            return new NodeDomElement(wpsConfig.getRequestXmlDoc().getNode(OPERATOR_PARAMETERS_XPATH));
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression", e);
        }
    }

}
