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

package com.bc.calvalus.processing;

import com.bc.calvalus.processing.shellexec.XmlDoc;
import com.bc.ceres.binding.dom.DomElement;
import com.bc.ceres.binding.dom.Xpp3DomElement;
import com.thoughtworks.xstream.io.copy.HierarchicalStreamCopier;
import com.thoughtworks.xstream.io.xml.DomReader;
import com.thoughtworks.xstream.io.xml.XppDomWriter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathExpressionException;

/**
 * Encapsulation the WPS XML configuration
 */
public class WpsConfig {
    private static final String TYPE_XPATH = "/Execute/Identifier";
    private static final String PROCESSOR_PACKAGE_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.processor.package']/Data/LiteralData";
    private static final String PROCESSOR_VERSION_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.processor.version']/Data/LiteralData";
    private static final String OUTPUT_DIR_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.output.dir']/Data/Reference/@href";
    private static final String OPERATOR_NAME_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.l2.operator']/Data/LiteralData";
    private static final String SYSTEM_PROPERTIES_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.system.properties']/Data/LiteralData";
    private static final String GEOMETRY_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.geometry']/Data/LiteralData";
    private static final String OPERATOR_PARAMETERS_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.l2.parameters']/Data/ComplexData/parameters";
    private static final String L3_PARAMETERS_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.l3.parameters']/Data/ComplexData";
    private static final String FORMATTER_PARAMETERS_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.formatter.parameters']/Data/ComplexData/parameters";
    private static final String INPUTS_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.input']";
    private static final String INPUT_FORMAT_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.input.format']/Data/LiteralData";
    private static final String INPUT_PATTERN_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.filenamepattern']";
    private static final String INPUT_HREF_XPATH = "Reference/@href";

    private final XmlDoc requestXmlDoc;
    private static final String PRIORITY_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.priority']/Data/LiteralData";

    public WpsConfig(String requestContent) {
        try {
            requestXmlDoc = new XmlDoc(requestContent);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create DOM: " + e.getMessage(), e);
        }
    }

    public XmlDoc getXmlDoc() {
        return requestXmlDoc;
    }

    public String getIdentifier() {
        try {
            return requestXmlDoc.getString(TYPE_XPATH);
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression: " + e.getMessage(), e);
        }
    }

    public String getProcessorPackage() {
        try {
            final String processorPackage = requestXmlDoc.getString(PROCESSOR_PACKAGE_XPATH);
            final String processorVersion = requestXmlDoc.getString(PROCESSOR_VERSION_XPATH);
            return processorPackage + "-" + processorVersion;
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression: " + e.getMessage(), e);
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
            throw new IllegalStateException("Illegal XPath expression: " + e.getMessage(), e);
        }
    }

    public String getFilenamePattern() {
        try {
            return requestXmlDoc.getString(INPUT_PATTERN_XPATH, (String)null);
        } catch (XPathExpressionException e) {
           throw new IllegalStateException("Illegal XPath expression: " + e.getMessage(), e);
        }
    }


    public String getRequestOutputDir() {
        try {
            return requestXmlDoc.getString(OUTPUT_DIR_XPATH);
        } catch (XPathExpressionException e) {
           throw new IllegalStateException("Illegal XPath expression: " + e.getMessage(), e);
        }
    }

    public String getOperatorName() {
        try {
            return requestXmlDoc.getString(OPERATOR_NAME_XPATH);
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression: " + e.getMessage(), e);
        }
    }

    public String getPriority() {
        try {
            return requestXmlDoc.getString(PRIORITY_XPATH, (String)null);
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression: " + e.getMessage(), e);
        }
    }

    public String getSystemProperties() {
        try {
            return requestXmlDoc.getString(SYSTEM_PROPERTIES_XPATH, (String)null);
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression: " + e.getMessage(), e);
        }
    }

    public String getLevel2Parameters() {
        try {
            Node node = requestXmlDoc.getNode(OPERATOR_PARAMETERS_XPATH);
            if (node != null) {
                return convertToDomElement(node).toXml();
            } else {
                return "";
            }
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression: " + e.getMessage(), e);
        }
    }

    public String getLevel3Parameters() {
        try {
            Node node = requestXmlDoc.getNode(L3_PARAMETERS_XPATH);
            if (node != null) {
                return convertToDomElement(node).toXml();
            } else {
                return "";
            }
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression: " + e.getMessage(), e);
        }
    }

    public String getFormatterParameters() {
        try {
            Node node = requestXmlDoc.getNode(FORMATTER_PARAMETERS_XPATH);
            if (node != null) {
                return convertToDomElement(node).toXml();
            } else {
                return "";
            }
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression: " + e.getMessage(), e);
        }
    }

    public static DomElement convertToDomElement(Node node) {
        Element element;
        if (node instanceof Element) {
            element = (Element) node;
        } else if (node instanceof Document) {
            element = ((Document) node).getDocumentElement();
        } else {
            throw new IllegalStateException("Cannot create DOM.");
        }
        XppDomWriter destination = new XppDomWriter();
        new HierarchicalStreamCopier().copy(new DomReader(element), destination);
        return new Xpp3DomElement(destination.getConfiguration());
    }

    public String getGeometry() {
        try {
            return requestXmlDoc.getString(GEOMETRY_XPATH, "");
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression: " + e.getMessage(), e);
        }
    }

    public String getInputFormat() {
        try {
            return requestXmlDoc.getString(INPUT_FORMAT_XPATH);
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression: " + e.getMessage(), e);
        }
    }
}
