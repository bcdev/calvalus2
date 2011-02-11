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
import com.bc.ceres.binding.ConversionException;
import com.bc.ceres.binding.PropertyContainer;
import com.bc.ceres.binding.PropertySet;
import com.bc.ceres.binding.ValidationException;
import com.bc.ceres.binding.dom.DefaultDomConverter;
import com.bc.ceres.binding.dom.DomElement;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.framework.gpf.Operator;
import org.esa.beam.framework.gpf.OperatorSpi;
import org.esa.beam.framework.gpf.annotations.ParameterDescriptorFactory;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class BeamOperatorConfiguration {

    private static final String TYPE_XPATH = "/Execute/Identifier";
    private static final String OUTPUT_DIR_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.output.dir']/Data/Reference/@href";
    private static final String PROCESSOR_PACKAGE_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.processor.package']/Data/LiteralData";
    private static final String PROCESSOR_VERSION_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.processor.version']/Data/LiteralData";
    private static final String OPERATOR_NAME_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.l2.operator']/Data/LiteralData";
    private static final String OPERATOR_PARAMETERS_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.l2.parameters']/Data/ComplexData/parameters";

    private final XmlDoc request;

    public BeamOperatorConfiguration(String requestContent) throws IOException, SAXException, ParserConfigurationException {
        request = new XmlDoc(requestContent);
    }

    public String getIdentifier() {
        try {
            return request.getString(TYPE_XPATH);
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression", e);
        }
    }

    public String getOperatorName() {
        try {
            return request.getString(OPERATOR_NAME_XPATH);
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression", e);
        }
    }

    public String getRequestOutputDir() {
        try {
            return request.getString(OUTPUT_DIR_XPATH);
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression", e);
        }
    }

    public String getProcessorPackage() throws XPathExpressionException {
        final String processorPackage = request.getString(PROCESSOR_PACKAGE_XPATH);
        final String processorVersion = request.getString(PROCESSOR_VERSION_XPATH);
        return processorPackage + "-" + processorVersion;
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
            Node parmatersNode = request.getNode(OPERATOR_PARAMETERS_XPATH);
            return new NodeDomElement(parmatersNode);
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression", e);
        }
    }

    /**
     * Note: this class is not thread save, nor may the input node be changed.
     */
    private static class NodeDomElement implements DomElement {
        private final Node node;
        private DomElement parent;
        private ArrayList<NodeDomElement> elementList;

        public NodeDomElement(Node node) {
            this.node = node;
        }

        @Override
        public String getName() {
            return node.getNodeName();
        }

        @Override
        public String getValue() {
            return node.getTextContent();
        }

        @Override
        public void setParent(DomElement parent) {
            this.parent = parent;
        }

        @Override
        public int getChildCount() {
            return getElementList().size();
        }

        @Override
        public DomElement getChild(int index) {
            return getElementList().get(index);
        }

        @Override
        public String toXml() {
            return "";
        }

        @Override
        public String getAttribute(String attributeName) {
            NamedNodeMap attributes = node.getAttributes();
            if (attributes == null) {
                return null;
            }
            Node namedItem = attributes.getNamedItem(attributeName);
            if (namedItem == null) {
                return null;
            }
            return namedItem.getNodeValue();
        }

        @Override
        public String[] getAttributeNames() {
            NamedNodeMap attributes = node.getAttributes();
            if (attributes == null) {
                return new String[0];
            }
            String[] attributeNames = new String[attributes.getLength()];
            for (int i = 0; i < attributeNames.length; i++) {
                attributeNames[i] = attributes.item(i).getNodeName();
            }
            return attributeNames;
        }

        @Override
        public DomElement getParent() {
            return parent;
        }

        @Override
        public DomElement getChild(String elementName) {
            List<NodeDomElement> elementList = getElementList();
            for (NodeDomElement nodeDomElement : elementList) {
                if (nodeDomElement.getName().equals(elementName)) {
                    return nodeDomElement;
                }
            }
            return null;
        }

        @Override
        public DomElement[] getChildren() {
            List<NodeDomElement> elementList = getElementList();
            return elementList.toArray(new DomElement[elementList.size()]);
        }

        @Override
        public DomElement[] getChildren(String elementName) {
            List<NodeDomElement> elementList = getElementList();
            List<DomElement> children = new ArrayList<DomElement>(elementList.size());
            for (NodeDomElement element : elementList) {
                if (element.getName().equals(elementName)) {
                    children.add(element);
                }
            }
            return children.toArray(new DomElement[children.size()]);
        }

        private List<NodeDomElement> getElementList() {
            if (elementList == null) {
                elementList = new ArrayList<NodeDomElement>();
                NodeList childNodes = node.getChildNodes();
                for (int i = 0; i < childNodes.getLength(); i++) {
                    Node childNode = childNodes.item(i);
                    short nodeType = childNode.getNodeType();
                    if (nodeType == 1) {
                        elementList.add(new NodeDomElement(childNode));
                    }
                }
            }
            return elementList;
        }

        @Override
        public void setAttribute(String name, String value) {
            throw new IllegalStateException("NodeDomElement is immutable.");
        }

        @Override
        public DomElement createChild(String name) {
            throw new IllegalStateException("NodeDomElement is immutable.");
        }

        @Override
        public void addChild(DomElement childElement) {
            throw new IllegalStateException("NodeDomElement is immutable.");
        }

        @Override
        public void setValue(String value) {
            throw new IllegalStateException("NodeDomElement is immutable.");
        }

    }
}
