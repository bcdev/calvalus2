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

import com.bc.calvalus.processing.shellexec.XmlDoc;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.util.StringUtils;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulation the WPS XML configuration
 */
public class WpsConfig {
    private static final String TYPE_XPATH = "/Execute/Identifier";
    private static final String PROCESSOR_PACKAGE_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.processor.package']/Data/LiteralData";
    private static final String PROCESSOR_VERSION_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.processor.version']/Data/LiteralData";
    private static final String OUTPUT_DIR_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.output.dir']/Data/Reference/@href";
    private static final String OPERATOR_NAME_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.l2.operator']/Data/LiteralData";
    private static final String OPERATOR_PARAMETERS_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.l2.parameters']/Data/ComplexData/parameters";
    private static final String L3_PARAMETERS_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.l3.parameters']/Data/ComplexData";
    private static final String INPUTS_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.input']";
    private static final String INPUT_HREF_XPATH = "Reference/@href";

    private final XmlDoc requestXmlDoc;

    public WpsConfig(String requestContent) throws IOException, SAXException, ParserConfigurationException {
        requestXmlDoc = new XmlDoc(requestContent);
    }

    public Map<String, String> getParametersMap() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(Config.CALVALUS_IDENTIFIER, getIdentifier());
        map.put(Config.CALVALUS_BUNDLE, getProcessorPackage());
        String[] requestInputPaths = getRequestInputPaths();
        String inputs = StringUtils.join(requestInputPaths, ",");
        map.put(Config.CALVALUS_INPUT, inputs);
        map.put(Config.CALVALUS_OUTPUT, getRequestOutputDir());
        map.put(Config.CALVALUS_L2_OPERATOR, getOperatorName());
        map.put(Config.CALVALUS_L2_PARAMETER, getLevel2Paramter());
        map.put(Config.CALVALUS_L3_PARAMETER, getLevel3Paramter());
        return map;
    }

    public XmlDoc getRequestXmlDoc() {
        return requestXmlDoc;
    }

    String getIdentifier() {
        try {
            return requestXmlDoc.getString(TYPE_XPATH);
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression", e);
        }
    }

    String getProcessorPackage() {
        try {
            final String processorPackage = requestXmlDoc.getString(PROCESSOR_PACKAGE_XPATH);
            final String processorVersion = requestXmlDoc.getString(PROCESSOR_VERSION_XPATH);
            return processorPackage + "-" + processorVersion;
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression", e);
        }
    }

    public String[] getRequestInputPaths() {
        try {
            NodeList nodes = requestXmlDoc.getNodes(INPUTS_XPATH);
            String[] inputPaths = new String[nodes.getLength()];
            for (int i = 0; i < nodes.getLength(); ++i) {
                // get input out of request
                Node node = nodes.item(i);
                String inputUrl = requestXmlDoc.getString(INPUT_HREF_XPATH, node);
                inputPaths[i] = inputUrl;
            }
            return inputPaths;
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression", e);
        }
    }


    String getRequestOutputDir() {
        try {
            return requestXmlDoc.getString(OUTPUT_DIR_XPATH);
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression", e);
        }
    }

    String getOperatorName() {
        try {
            return requestXmlDoc.getString(OPERATOR_NAME_XPATH);
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression", e);
        }
    }

    boolean isLevel3() {
        try {
            Node node = requestXmlDoc.getNode(L3_PARAMETERS_XPATH);
            if (node != null) {
                return true;
            }
        } catch (XPathExpressionException ignore) {
        }
        return false;
    }

    private String getLevel2Paramter() {
        try {
            Node node = requestXmlDoc.getNode(OPERATOR_PARAMETERS_XPATH);
            NodeDomElement nodeDomElement = new NodeDomElement(node);
            return nodeDomElement.toXml();
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression", e);
        }
    }

    private String getLevel3Paramter() {
        try {
            Node node = requestXmlDoc.getNode(L3_PARAMETERS_XPATH);
            if (node != null) {
                NodeDomElement nodeDomElement = new NodeDomElement(node);
                return nodeDomElement.toXml();
            } else {
                return null;
            }
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression", e);
        }
    }

   public static WpsConfig createFromJobConfig(Configuration hadoopConfiguration) {
        final String requestContent = hadoopConfiguration.get("calvalus.request");
        try {
            return new WpsConfig(requestContent);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static void addToConfiguration(Map<String, String> parametersMap, Configuration configuration) {
        for (Map.Entry<String, String> entry : parametersMap.entrySet()) {
            String value = entry.getValue();
            if (value != null) {
                configuration.set(entry.getKey(), value);
            }
        }
    }
}
